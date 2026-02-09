"""
Face Processor - Handles face detection and embedding extraction.

Uses InsightFace with buffalo_l model for high-quality face recognition.
Integrates with Phase 1 database for storing embeddings.
"""
import logging
import asyncio
import numpy as np
import cv2
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from PIL import Image
import io
import time
from config import settings, get_dynamic_setting

logger = logging.getLogger(__name__)


class FaceProcessor:
    """
    Handles face detection and embedding extraction using InsightFace.
    Runs detection in thread pool to avoid blocking asyncio event loop.
    """
    
    _instance = None
    _executor = ThreadPoolExecutor(max_workers=2)
    _initialized = False
    
    def __init__(self, providers: list = None):
        """
        Initialize InsightFace model with retry logic.
        
        Args:
            providers: ONNX Runtime providers. Defaults to CPU, set to 
                      ['CUDAExecutionProvider'] for GPU acceleration.
        """
        self.providers = providers
        self.app = None
        self.min_quality = 0.3  # Default, will be overwritten
        
    def _lazy_init(self):
        """Lazy initialization of InsightFace model with retry logic."""
        if self._initialized:
            return True
            
        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            try:
                from insightface.app import FaceAnalysis

                # Use environment variable or default to CPU
                if self.providers is None:
                    gpu_enabled = settings.USE_GPU
                    if gpu_enabled:
                        self.providers = ['CUDAExecutionProvider', 'CPUExecutionProvider']
                    else:
                        self.providers = ['CPUExecutionProvider']
                
                self.app = FaceAnalysis(name='buffalo_l', providers=self.providers)
                self.app.prepare(ctx_id=0, det_size=(640, 640))
                
                # Quality threshold from environment
                self.min_quality = settings.MIN_QUALITY_THRESHOLD
                
                self._initialized = True
                logger.info(f"InsightFace initialized. Providers: {self.providers}, Min quality: {self.min_quality}")
                return True
                
            except Exception as e:
                logger.error(f"FaceProcessor init failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error("FaceProcessor initialization failed after max retries")
                    return False
        
        return False
    
    @classmethod
    def get_instance(cls) -> 'FaceProcessor':
        """Gets or creates singleton instance."""
        if cls._instance is None:
            cls._instance = FaceProcessor()
        # Ensure lazy init is done
        cls._instance._lazy_init()
        return cls._instance
    
    async def process_image(self, image_input) -> List[Dict]:
        """
        Detects faces in an image and extracts embeddings.
        
        Args:
            image_input: Can be numpy array (BGR), PIL Image, or BytesIO buffer
            
        Returns:
            List of face dictionaries with embedding, bbox, quality, landmarks
        """
        # Convert input to numpy array if needed
        image_array = self._to_numpy(image_input)
        if image_array is None:
            return []
        
        try:
            # Run detection in thread pool (CPU-bound operation)
            loop = asyncio.get_event_loop()
            faces = await loop.run_in_executor(
                self._executor, 
                self._detect_faces_sync, 
                image_array
            )
            return faces
            
        except (MemoryError, RuntimeError) as e:
            logger.error(f"Face processing failed (resource error): {e}")
            # Save for later retry if it's a resource issue
            await self._save_for_retry(image_input)
            return []
        except Exception as e:
            logger.error(f"Face processing unexpected error: {e}")
            return []
            
    async def _save_for_retry(self, image_input):
        """Saves failed image for later processing (e.g. after restart)."""
        try:
            # Save to disk as fallback
            timestamp = int(time.time())
            retry_dir = "failed_media_retry"
            os.makedirs(retry_dir, exist_ok=True)
            
            filename = f"{retry_dir}/failed_{timestamp}.bin"
            
            # Get bytes
            if isinstance(image_input, (io.BytesIO, bytes)):
                data = image_input.getvalue() if isinstance(image_input, io.BytesIO) else image_input
                with open(filename, "wb") as f:
                    f.write(data)
                logger.info(f"Saved failed media to {filename} for later retry")
        except Exception as e:
            logger.error(f"Failed to save media for retry: {e}")
    
    def _to_numpy(self, image_input) -> Optional[np.ndarray]:
        """Converts various image formats to numpy array (BGR)."""
        try:
            if isinstance(image_input, np.ndarray):
                # Already numpy, ensure BGR format
                if len(image_input.shape) == 3 and image_input.shape[2] == 3:
                    return image_input
                return None
            
            elif isinstance(image_input, Image.Image):
                # PIL Image -> numpy BGR
                rgb_array = np.array(image_input)
                return cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)
            
            elif isinstance(image_input, (io.BytesIO, bytes)):
                # BytesIO or bytes -> PIL -> numpy
                if isinstance(image_input, bytes):
                    image_input = io.BytesIO(image_input)
                image_input.seek(0)
                pil_image = Image.open(image_input)
                rgb_array = np.array(pil_image.convert('RGB'))
                return cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)
            
            else:
                logger.warning(f"Unsupported image type: {type(image_input)}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to convert image to numpy: {e}")
            return None
    
    def _detect_faces_sync(self, image_array: np.ndarray) -> List[Dict]:
        """
        Synchronous face detection (called from thread pool).
        
        Returns list of face dicts with:
        - embedding: 512-dim normalized embedding
        - bbox: [x1, y1, x2, y2] bounding box
        - quality: detection confidence score
        - landmarks: facial landmark points (optional)
        """
        try:
            faces = self.app.get(image_array)
            results = []
            
            for face in faces:
                # Quality filtering
                dynamic_quality = get_dynamic_setting("MIN_QUALITY_THRESHOLD", self.min_quality)
                
                quality = float(face.det_score)
                if quality < dynamic_quality:
                    logger.debug(f"Skipping low-quality face: {quality:.3f} < {dynamic_quality}")
                    continue
                
                face_dict = {
                    'embedding': face.normed_embedding.tolist(),
                    'bbox': face.bbox.tolist(),
                    'quality': quality,
                }
                
                # Include landmarks if available
                if hasattr(face, 'landmark_2d_106') and face.landmark_2d_106 is not None:
                    face_dict['landmarks'] = face.landmark_2d_106.tolist()
                elif hasattr(face, 'kps') and face.kps is not None:
                    face_dict['landmarks'] = face.kps.tolist()
                
                results.append(face_dict)
            
            logger.debug(f"Detected {len(results)} faces (filtered from {len(faces)})")
            return results
            
        except Exception as e:
            logger.error(f"Face detection failed: {e}")
            return []
    
    async def process_frames(self, frames: List) -> List[Dict]:
        """
        Processes multiple video frames and aggregates faces.
        
        Args:
            frames: List of frames (numpy arrays or PIL Images)
            
        Returns:
            List of all detected faces across all frames
        """
        all_faces = []
        
        for i, frame in enumerate(frames):
            faces = await self.process_image(frame)
            
            # Add frame info to each face
            for face in faces:
                face['frame_index'] = i
            
            all_faces.extend(faces)
        
        logger.info(f"Processed {len(frames)} frames, found {len(all_faces)} faces")
        return all_faces
    
    def get_embedding_vector(self, face_dict: Dict) -> List[float]:
        """Extracts the embedding vector from a face dictionary."""
        return face_dict.get('embedding', [])
    
    def calculate_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """
        Calculates cosine similarity between two embeddings.
        
        Returns:
            Similarity score between 0 and 1 (1 = identical)
        """
        vec1 = np.array(embedding1)
        vec2 = np.array(embedding2)
        
        # Cosine similarity
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))

