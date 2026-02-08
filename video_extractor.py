"""
Video Frame Extractor - Adaptive frame extraction from video files.

Uses PyAV for efficient video processing with adaptive strategies
based on video type and duration.
"""
import logging
import asyncio
import io
import os
from typing import List, Generator
from concurrent.futures import ThreadPoolExecutor
import numpy as np

logger = logging.getLogger(__name__)


class VideoFrameExtractor:
    """
    Extracts frames from video files using adaptive strategies.
    
    Strategies:
    - Round videos (video notes): 2 fps for denser sampling
    - Short videos (<30s): 1 fps fixed rate
    - Long videos: Keyframe extraction with fallback
    """
    
    _executor = ThreadPoolExecutor(max_workers=2)
    
    def __init__(self):
        from config import settings
        import av
        self.av = av
        
        # Configuration from environment
        # Note: VIDEO_FRAME_RATE not currently in Settings, keeping logic or adding to Settings
        # Adding simple fallback for now or we can add to Settings class if critical
        self.default_fps = 1.0 
        self.round_video_fps = 2.0  # Higher sampling for round videos
        self.max_frames = 100  # Cap to prevent memory issues
        
        logger.info(f"VideoFrameExtractor initialized. Default FPS: {self.default_fps}")
    
    async def extract_frames(
        self, 
        video_buffer: io.BytesIO, 
        is_round_video: bool = False
    ) -> List[np.ndarray]:
        """
        Extracts frames from a video buffer.
        
        Args:
            video_buffer: BytesIO containing video data
            is_round_video: True if this is a Telegram video note (round video)
            
        Returns:
            List of numpy arrays in BGR format for face processing
        """
        loop = asyncio.get_event_loop()
        frames = await loop.run_in_executor(
            self._executor,
            self._extract_frames_sync,
            video_buffer,
            is_round_video
        )
        return frames
    
    def _extract_frames_sync(
        self, 
        video_buffer: io.BytesIO, 
        is_round_video: bool
    ) -> List[np.ndarray]:
        """Synchronous frame extraction (runs in thread pool)."""
        try:
            video_buffer.seek(0)
            container = self.av.open(video_buffer)
            
            # Get video stream
            stream = container.streams.video[0]
            stream.thread_type = 'AUTO'
            
            # Calculate video duration
            duration = float(stream.duration * stream.time_base) if stream.duration else 30.0
            
            # Determine extraction strategy
            if is_round_video:
                frames = self._extract_at_fps(container, stream, self.round_video_fps)
            elif duration < 30:
                frames = self._extract_at_fps(container, stream, self.default_fps)
            else:
                frames = self._extract_keyframes(container, stream)
            
            container.close()
            
            # Convert to BGR numpy arrays
            bgr_frames = []
            for frame in frames[:self.max_frames]:
                rgb_array = frame.to_ndarray(format='rgb24')
                # RGB to BGR for OpenCV/InsightFace
                import cv2
                bgr_array = cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)
                bgr_frames.append(bgr_array)
            
            logger.info(f"Extracted {len(bgr_frames)} frames (round={is_round_video}, duration={duration:.1f}s)")
            return bgr_frames
            
        except Exception as e:
            logger.error(f"Frame extraction failed: {e}")
            return []
    
    def _extract_at_fps(self, container, stream, target_fps: float) -> List:
        """Extracts frames at a fixed FPS rate."""
        frames = []
        
        video_fps = float(stream.average_rate) if stream.average_rate else 30.0
        interval = max(1, int(video_fps / target_fps))
        
        for i, frame in enumerate(container.decode(stream)):
            if i % interval == 0:
                frames.append(frame)
                
            if len(frames) >= self.max_frames:
                break
        
        return frames
    
    def _extract_keyframes(self, container, stream) -> List:
        """Extracts only keyframes (I-frames) for long videos."""
        frames = []
        
        # First, try to decode only keyframes
        for packet in container.demux(stream):
            if packet.is_keyframe:
                for frame in packet.decode():
                    frames.append(frame)
                    if len(frames) >= self.max_frames:
                        break
            
            if len(frames) >= self.max_frames:
                break
        
        # If too few keyframes, fall back to fixed rate
        if len(frames) < 5:
            logger.debug("Too few keyframes, falling back to fixed rate extraction")
            container.seek(0)
            frames = self._extract_at_fps(container, stream, 0.5)  # 1 frame per 2 sec
        
        return frames
    
    def extract_frames_generator(
        self, 
        video_buffer: io.BytesIO,
        target_fps: float = 1.0
    ) -> Generator[np.ndarray, None, None]:
        """
        Memory-efficient generator that yields frames one at a time.
        
        Use this for very large videos to avoid loading all frames into memory.
        """
        try:
            video_buffer.seek(0)
            container = self.av.open(video_buffer)
            stream = container.streams.video[0]
            stream.thread_type = 'AUTO'
            
            video_fps = float(stream.average_rate) if stream.average_rate else 30.0
            interval = max(1, int(video_fps / target_fps))
            
            import cv2
            
            for i, frame in enumerate(container.decode(stream)):
                if i % interval == 0:
                    rgb_array = frame.to_ndarray(format='rgb24')
                    bgr_array = cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)
                    yield bgr_array
            
            container.close()
            
        except Exception as e:
            logger.error(f"Frame generator failed: {e}")
