-- This script modifies the existing database schema to allow multiple uploads of the same message
-- to DIFFERENT topics (e.g. if a photo has 2 faces, it can go to 2 topics).
-- It preserves existing data.

BEGIN;

-- 1. Remove the old strict constraint (one upload per message globally)
ALTER TABLE uploaded_media 
DROP CONSTRAINT IF EXISTS uploaded_media_source_chat_id_source_message_id_key;

-- 2. Add the new flexible constraint (one upload per message PER TOPIC)
ALTER TABLE uploaded_media 
ADD CONSTRAINT uploaded_media_source_chat_id_source_message_id_topic_id_key 
UNIQUE (source_chat_id, source_message_id, topic_id);

COMMIT;
