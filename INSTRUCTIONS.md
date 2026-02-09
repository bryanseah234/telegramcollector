
# INSTRUCTIONS.md

## Instructions for AI Coding Agent (Antigravity)

This document provides specific instructions for implementing the Face Recognition Archive System using an AI coding agent. It includes context, constraints, and detailed guidance for successful implementation.

---

## Project Context

You are building a Telegram-based face recognition archival system that:

- Scans Telegram chat histories for photos and videos
- Extracts faces using InsightFace AI models
- Clusters faces by identity using vector similarity search
- Organizes media into Telegram forum topics, one per person
- Provides a Streamlit dashboard for human oversight and corrections
- Runs in Docker containers for consistent deployment
- Automatically updates when new code is pushed to GitHub

**Primary Use Case:** Community safety and identifying bad actors/scammers across multiple Telegram groups.

**Key Constraints:**

- Minimal disk usage (process in memory, no temp files)
- 16GB RAM, <512GB disk
- PostgreSQL with pgvector for vector storage
- Must handle millions of images at scale
- Prioritize recall over precision (avoid missing matches)

---

## Architecture Overview

```
┌─────────────────┐
│  Telegram API   │
└────────┬────────┘
         │
    ┌────▼────┐
    │ Scanner │──────┐
    └─────────┘      │
                     │
    ┌────────────────▼──────┐
    │  Processing Queue     │
    │  ┌─────────────────┐  │
    │  │ Download Media  │  │
    │  ├─────────────────┤  │
    │  │ Extract Frames  │  │
    │  ├─────────────────┤  │
    │  │ Detect Faces    │  │
    │  ├─────────────────┤  │
    │  │ Match Identity  │  │
    │  ├─────────────────┤  │
    │  │ Upload to Topic │  │
    │  └─────────────────┘  │
    └───────────┬───────────┘
                │
         ┌──────▼──────┐
         │  PostgreSQL │
         │  + pgvector │
         └──────┬──────┘
                │
         ┌──────▼──────┐
         │  Dashboard  │
         │ (Streamlit) │
         └─────────────┘
```

---

## Implementation Approach

### Step 1: Read All Documentation

Before writing any code:

1. Read all 5 parts of the PRD (Parts 1-5 in the conversation)
2. Read TASKS.md completely
3. Understand the data flow and component interactions
4. Identify dependencies between tasks

### Step 2: Set Up Environment

1. Create project directory structure:

```
face-archiver/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.template
├── .gitignore
├── init-db.sql
├── README.md
├── worker.py              # Main application entry point
├── config.py              # Centralized configuration
├── database.py
├── telegram_client.py
├── message_scanner.py
├── topic_manager.py
├── media_downloader.py
├── media_uploader.py
├── face_processor.py
├── video_extractor.py
├── identity_matcher.py
├── processing_queue.py
├── login_bot.py
├── bot_client.py
├── dashboard.py
├── reset_and_update.bat   # Helper for Windows updates
├── reset_and_update.sh    # Helper for Linux updates
└── tests/
    └── test_integration.py
```

### Step 3: Implement in Phases

Follow the task order in TASKS.md:

- Phase 1: Foundation (database, config)
- Phase 2: Telegram (client, scanner, topics)
- Phase 3: Processing (download, detect, match)
- Phase 4: Dashboard (UI, corrections)
- Phase 5: Deployment (Docker, scripts)
- Phase 6: Testing (validation)

### Step 4: Test Each Component

After completing each task:

1. Run the verification steps from TASKS.md
2. Fix any issues before proceeding
3. Mark the task complete in the checklist

---

## Critical Implementation Details

### Database Schema

- **MUST** enable pgvector extension first
- **MUST** create IVFFlat index on embeddings (lists=1000)
- Foreign keys use CASCADE deletion for cleanup
- Use SERIAL for auto-incrementing IDs
- All timestamps use `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

### Telegram Client

- Session files MUST persist in `/app/sessions/` volume
- Use Telethon, not Pyrogram (better InputPeer handling)
- Handle FloodWaitError with `await asyncio.sleep(error.seconds)`
- NEVER hardcode credentials - always use environment variables

### Face Processing

- Use InsightFace buffalo_l model (NOT buffalo_s)
- Run face detection in thread pool: `await loop.run_in_executor(None, func)`
- Quality threshold: 0.5 minimum
- Similarity threshold: 0.55 for matching
- Embeddings MUST be 512-dimensional vectors

### Video Processing

- Process in memory using PyAV + BytesIO
- Adaptive frame extraction:
  - Round videos: 2 fps
  - Short (<30s): 1 fps
  - Long: Try keyframes, fallback to 1fps if sparse
- NEVER write video to disk

### Vector Search

- Use cosine distance: `embedding <=> query::vector`
- Convert to similarity: `1 - distance`
- Query returns closest match first (ORDER BY distance ASC)
- Index required for performance with 100k+ vectors

### Processing Queue

- 3 concurrent workers by default
- Deduplicate faces within same video (similarity > 0.95)
- Queue max size: 1000 messages
- Graceful shutdown: wait for current batch to complete

### Dashboard

- Use `@st.cache_resource` for FaceProcessor (loads model once)
- Connection pool shared with main app
- Session state for navigation between views
- Plotly for interactive charts (not matplotlib)

---

## Code Style and Conventions

### Python Standards

```python
# Use type hints
def process_image(image: np.ndarray) -> List[Dict[str, Any]]:
    pass

# Use docstrings (Google style)
def find_match(embedding: List[float]) -> Optional[Tuple[int, float]]:
    """
    Searches for a matching identity.
    
    Args:
        embedding: 512-dimensional face embedding
        
    Returns:
        Tuple of (topic_id, similarity) if match found, None otherwise
    """
    pass

# Use logging, not print
import logging
logger = logging.getLogger(__name__)
logger.info("Processing started")

# Handle errors gracefully
try:
    result = risky_operation()
except SpecificException as e:
    logger.error(f"Operation failed: {e}")
    return None
```

### Async/Await Patterns

```python
# Always await async functions
result = await async_function()

# Use asyncio.gather for concurrent operations
results = await asyncio.gather(
    task1(),
    task2(),
    task3()
)

# Use asyncio.sleep for delays, not time.sleep
await asyncio.sleep(seconds)

# Use context managers for resources
async with client:
    await client.do_something()
```

### Database Patterns

```python
# Always use async context manager
async with get_db_connection() as conn:
    async with conn.cursor() as cur:
        await cur.execute(query, params)
        # Note: autocommit via pool kwargs, or conn.commit() if manual transaction handling

# Use parameterized queries (prevent SQL injection)
await cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))

# Convert lists to PostgreSQL format for pgvector
embedding_str = '[' + ','.join(map(str, embedding)) + ']'
await cur.execute("INSERT INTO embeddings (vec) VALUES (%s::vector)", (embedding_str,))
```

---

## Common Pitfalls to Avoid

### ❌ DON'T

```python
# Don't write temp files
with open('/tmp/video.mp4', 'wb') as f:
    f.write(video_data)

# Don't use print for logging
print("Processing message...")

# Don't hardcode credentials
client = TelegramClient('session', 12345678, 'abc123')

# Don't block the event loop with CPU work
faces = face_app.get(image)  # Blocks asyncio!

# Don't forget to close resources
buffer = io.BytesIO(data)
# ... use buffer ...
# Buffer never closed - memory leak!
```

### ✅ DO

```python
# Process in memory
buffer = io.BytesIO(video_data)
frames = extract_frames(buffer)
buffer.close()

# Use logging module
logger.info("Processing message %s", message_id)

# Use environment variables
api_id = int(os.getenv('TG_API_ID'))

# Run CPU work in thread pool
faces = await loop.run_in_executor(None, face_app.get, image)

# Always clean up resources
try:
    buffer = io.BytesIO(data)
    # ... use buffer ...
finally:
    buffer.close()
```

---

## Testing Strategy

### Unit Tests

Test individual components in isolation:

```python
# test_face_processor.py
async def test_face_detection():
    processor = FaceProcessor()
    test_image = load_test_image("face.jpg")
    
    faces = await processor.process_image(test_image)
    
    assert len(faces) == 1
    assert len(faces[0]['embedding']) == 512
    assert 0 <= faces[0]['quality'] <= 1
```

### Integration Tests

Test component interactions:

```python
# test_integration.py
async def test_complete_workflow():
    # Setup
    client = await initialize_test_client()
    queue = setup_processing_queue()
    
    # Execute
    test_message = create_test_message_with_face()
    await queue.enqueue(test_message, account_id=1, chat_id=-100123)
    await asyncio.sleep(5)  # Wait for processing
    
    # Verify
    assert queue.stats['faces_detected'] == 1
    
    # Check database
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM face_embeddings")
        assert cursor.fetchone()[0] == 1
```

### Manual Testing

After deployment:

1. Message the bot with `/start` to register an account
2. System auto-discovers and scans all chats
3. Monitor logs: `docker-compose logs -f app`
4. Check dashboard at <http://localhost:8501>
5. Verify topics created in Telegram hub group
6. Test manual corrections (merge/split/rename)

---

## Debugging Guide

### Container Won't Start

```bash
# Check logs
docker-compose logs app

# Common issues:
# - Missing environment variable → Check .env file
# - Database not ready → Wait for postgres health check
# - Import error → Check requirements.txt versions
```

### Face Detection Not Working

```bash
# Check model loading
docker-compose exec app python -c "from face_processor import FaceProcessor; p = FaceProcessor()"

# Common issues:
# - Model download failed → Rebuild image
# - CPU/memory limits too low → Increase in docker-compose.yml
# - Wrong image format → Convert to BGR before processing
```

### Database Queries Slow

```sql
-- Check if vector index exists
\d face_embeddings

-- Rebuild index if missing
REINDEX INDEX idx_embeddings_vector;

-- Check query plan
EXPLAIN ANALYZE 
SELECT * FROM face_embeddings 
ORDER BY embedding <=> '[0.1,0.2,...]'::vector 
LIMIT 1;
```

### Memory Issues

```bash
# Check container memory usage
docker stats

# Common causes:
# - Too many workers → Reduce in processing_queue.py
# - Video too large → Lower MAX_MEDIA_SIZE_MB
# - Memory leak → Check buffer.close() calls
```

---

## Deployment Checklist

Before deploying to production:

- [ ] All environment variables set in .env
- [ ] Strong PostgreSQL password generated
- [ ] Telegram API credentials obtained
- [ ] Hub group created with forum topics enabled
- [ ] Docker and Docker Compose installed
- [ ] Sufficient disk space available (100GB+ recommended)
- [ ] Sufficient RAM available (16GB minimum)
- [ ] Firewall configured (allow port 8501 for dashboard)
- [ ] SSL certificate for dashboard (optional but recommended)
- [ ] Backup strategy defined
- [ ] Monitoring configured (optional)
- [ ] Documentation reviewed
- [ ] Test account authenticated
- [ ] Test chat added and processing successfully

---

## Performance Optimization

### If Processing Too Slow

1. Increase worker count in processing_queue.py
2. Reduce video frame extraction rate
3. Increase MAX_MEDIA_SIZE_MB to skip large files
4. Use GPU for face detection (requires different Docker image)

### If Database Queries Slow

1. Ensure vector index exists and is healthy
2. Increase IVFFlat lists parameter (rebuild index)
3. Increase PostgreSQL shared_buffers
4. Consider read replicas for dashboard queries

### If Memory Usage Too High

1. Reduce worker count
2. Lower MAX_MEDIA_SIZE_MB
3. Implement stricter quality filtering
4. Process shorter time windows for backfill

---

## Security Considerations

### Secrets Management

- NEVER commit .env to git
- Use strong random passwords (openssl rand -base64 32)
- Rotate credentials regularly
- Limit PostgreSQL port exposure (remove from docker-compose.yml in production)

### Network Security

- Run dashboard behind reverse proxy with HTTPS
- Use firewall to restrict access
- Consider VPN for remote access
- Disable PostgreSQL external port in production

### Data Privacy

- Obtain proper consent before scanning chats
- Comply with GDPR/CCPA requirements
- Implement data retention policies
- Provide data deletion mechanisms
- Audit access to sensitive data

---

## Maintenance Procedures

### Daily

- Check dashboard for errors
- Monitor disk space usage
- Review processing statistics

### Weekly

- Review and address accumulated errors
- Check database size and performance
- Verify backups are current
- Run Updates: Use `reset_and_update.bat` (Windows) or `./reset_and_update.sh` (Linux) to pull latest code and reset database if needed.

### Monthly

- Run database maintenance (VACUUM ANALYZE)
- Clean old processing metrics
- Review and optimize slow queries
- Update dependencies if needed

### Quarterly

- Review and update documentation
- Audit security configurations
- Test disaster recovery procedures
- Evaluate performance trends

---

## Support and Troubleshooting

### Common Error Messages

**"No module named 'insightface'"**

- Solution: Rebuild Docker image, ensure requirements.txt includes insightface

**"could not connect to server: Connection refused"**

- Solution: Wait for PostgreSQL to start (health check), verify connection string

**"FloodWaitError: A wait of X seconds is required"**

- Solution: Normal rate limiting, application handles automatically

**"vector dimension mismatch"**

- Solution: Verify using buffalo_l model (512 dimensions), not buffalo_s

**"Out of memory"**

- Solution: Reduce workers, lower media size limit, increase Docker memory limit

### Getting Help

1. Check logs: `docker-compose logs app`
2. Review error messages carefully
3. Consult relevant PRD section
4. Search error message online
5. Check GitHub issues (if project is public)
6. Verify configuration matches specification

---

## Success Criteria

The implementation is complete when:

✅ All tasks in TASKS.md are checked off
✅ All verification steps pass
✅ Integration tests pass
✅ System processes test images successfully
✅ Dashboard displays data correctly
✅ Manual corrections work (merge/split/rename)
✅ System recovers gracefully from restarts
✅ Documentation is complete and accurate
✅ Deployment works on fresh server following README

---

## Final Notes for AI Agent

**Remember:**

1. Read the full PRD (Parts 1-5) before starting
2. Implement tasks in order (respect dependencies)
3. Verify each task before moving to next
4. Ask for clarification if requirements unclear
5. Test incrementally, not just at the end
6. Document all functions and classes
7. Handle errors gracefully with logging
8. Use type hints throughout
9. Follow Python best practices
10. Keep code simple and readable

**The goal is a production-ready system that:**

- Actually works end-to-end
- Handles errors gracefully
- Can be deployed by following documentation
- Can be maintained by other developers
- Performs acceptably at scale
