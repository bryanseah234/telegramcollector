import logging
import io
import os
import av
import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

class MediaManager:
    """
    Handles downloading and processing of media files (photos/videos).
    """
    def __init__(self, max_size_mb=200):
        self.max_size_bytes = max_size_mb * 1024 * 1024

    async def download_media(self, message) -> io.BytesIO:
        """
        Downloads media (photo or video) from a message into memory.
        """
        try:
            # Check file size if available
            if hasattr(message, 'file') and message.file.size > self.max_size_bytes:
                logger.warning(f"File too large: {message.file.size} bytes. Skipping.")
                return None

            buffer = io.BytesIO()
            await message.download_media(file=buffer)
            buffer.seek(0)
            return buffer
        except Exception as e:
            logger.error(f"Download failed for message {message.id}: {e}")
            return None

    def extract_frames(self, video_buffer: io.BytesIO, sample_rate: float = 1.0):
        """
        Extracts frames from a video buffer at a given sample rate (fps).
        Returns a generator of PIL Images.
        """
        try:
            container = av.open(video_buffer)
            stream = container.streams.video[0]
            stream.thread_type = 'AUTO'
            
            fps = float(stream.average_rate)
            interval = int(fps / sample_rate) if sample_rate > 0 else 1
            
            for i, frame in enumerate(container.decode(stream)):
                if i % interval == 0:
                    yield frame.to_image()
                    
        except Exception as e:
            logger.error(f"Failed to extract frames: {e}")
