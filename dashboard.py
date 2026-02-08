"""
Telegram Face Recognition Dashboard

Streamlit-based dashboard for monitoring and managing the face recognition system.
Implements Phase 4 tasks: statistics, identity gallery, detail view, and face search.
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import psycopg
from config import settings
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    """
    Synchronous database connection for Streamlit.
    """
    conn_str = (
        f"host={settings.DB_HOST} "
        f"port={settings.DB_PORT} "
        f"dbname={settings.DB_NAME} "
        f"user={settings.DB_USER} "
        f"password={settings.DB_PASSWORD}"
    )
    conn = psycopg.connect(conn_str, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()

# Page configuration
st.set_page_config(
    page_title="Face Recognition Dashboard",
    page_icon="👤",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern styling
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    }
    
    /* Metric cards */
    .stMetric {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 16px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    /* Identity cards */
    .identity-card {
        background: rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
        border: 1px solid rgba(255, 255, 255, 0.12);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .identity-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    }
    
    /* Status indicators */
    .status-ok { color: #00d26a; }
    .status-warning { color: #ffaa00; }
    .status-error { color: #ff4757; }
    
    /* Section headers */
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        margin: 24px 0 16px 0;
        color: #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)


# ============================================
# Database Query Functions
# ============================================

@st.cache_data(ttl=30)
def get_statistics():
    """Fetches main dashboard statistics."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Total identities
        cursor.execute("SELECT COUNT(*) FROM telegram_topics")
        total_identities = cursor.fetchone()[0]
        
        # Total embeddings
        cursor.execute("SELECT COUNT(*) FROM face_embeddings")
        total_embeddings = cursor.fetchone()[0]
        
        # Total messages uploaded
        cursor.execute("SELECT COUNT(*) FROM uploaded_media")
        total_messages = cursor.fetchone()[0]
        
        # Active accounts
        cursor.execute("SELECT COUNT(*) FROM telegram_accounts WHERE status = 'active'")
        active_accounts = cursor.fetchone()[0]
        
        # Processed media count
        cursor.execute("SELECT COUNT(*) FROM processed_media")
        processed_media = cursor.fetchone()[0]
        
        # Duplicates skipped (media with same file_unique_id seen before)
        cursor.execute("""
            SELECT COUNT(*) FROM processed_media 
            WHERE processed_at > NOW() - INTERVAL '24 hours'
        """)
        recent_processed = cursor.fetchone()[0]
        
        return {
            'identities': total_identities,
            'embeddings': total_embeddings,
            'messages': total_messages,
            'accounts': active_accounts,
            'processed_media': processed_media,
            'recent_processed': recent_processed
        }

@st.cache_data(ttl=30)
def get_recent_activity():
    """Gets processing activity for the last 24 hours."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date_trunc('hour', processed_at) as hour,
                COUNT(*) as count
            FROM processed_media
            WHERE processed_at > NOW() - INTERVAL '24 hours'
            GROUP BY date_trunc('hour', processed_at)
            ORDER BY hour
        """)
        rows = cursor.fetchall()
        
        if not rows:
            return pd.DataFrame({'hour': [], 'count': []})
        
        return pd.DataFrame(rows, columns=['hour', 'count'])

@st.cache_data(ttl=30)
def get_scan_progress():
    """Gets scanning progress for all chats."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                chat_title,
                processed_messages,
                total_messages,
                scan_mode,
                last_updated
            FROM scan_checkpoints
            ORDER BY last_updated DESC
            LIMIT 20
        """)
        rows = cursor.fetchall()
        
        return pd.DataFrame(rows, columns=[
            'Chat', 'Processed', 'Total', 'Mode', 'Last Updated'
        ])

@st.cache_data(ttl=60)
def get_recent_errors():
    """Gets recent processing errors."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT error_type, error_message, error_timestamp
            FROM processing_errors
            ORDER BY error_timestamp DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        
        return pd.DataFrame(rows, columns=['Type', 'Message', 'Time'])

@st.cache_data(ttl=60)
def get_health_status():
    """Gets the latest health check status."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                check_time, database_ok, telegram_ok, 
                face_model_ok, hub_access_ok, queue_size,
                workers_active, error_message
            FROM health_checks
            ORDER BY check_time DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        if row:
            return {
                'check_time': row[0],
                'database': row[1],
                'telegram': row[2],
                'face_model': row[3],
                'hub_access': row[4],
                'queue_size': row[5],
                'workers': row[6],
                'error': row[7]
            }
        return None

@st.cache_data(ttl=30)
def get_identities(page: int = 1, per_page: int = 12, search: str = "", sort_by: str = "last_seen"):
    """Fetches identities for the gallery view."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Build query
        where_clause = ""
        params = []
        
        if search:
            where_clause = "WHERE label ILIKE %s OR CAST(id AS TEXT) LIKE %s"
            params = [f"%{search}%", f"%{search}%"]
        
        # Order by
        order_map = {
            'last_seen': 'last_seen DESC',
            'first_seen': 'first_seen ASC',
            'name': 'label ASC',
            'faces': 'embedding_count DESC'
        }
        order = order_map.get(sort_by, 'last_seen DESC')
        
        offset = (page - 1) * per_page
        
        query = f"""
            SELECT id, topic_id, label, first_seen, last_seen, 
                   embedding_count, message_count, exemplar_image_url
            FROM telegram_topics
            {where_clause}
            ORDER BY {order}
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM telegram_topics {where_clause}"
        cursor.execute(count_query, params[:-2] if params else [])
        total = cursor.fetchone()[0]
        
        return rows, total

@st.cache_data(ttl=30)
def get_identity_detail(identity_id: int):
    """Fetches detailed information for a single identity."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Identity info
        cursor.execute("""
            SELECT id, topic_id, label, first_seen, last_seen,
                   embedding_count, message_count, exemplar_image_url
            FROM telegram_topics
            WHERE id = %s
        """, (identity_id,))
        identity = cursor.fetchone()
        
        if not identity:
            return None, []
        
        # All embeddings for this identity
        cursor.execute("""
            SELECT id, source_chat_id, source_message_id, 
                   quality_score, detection_timestamp
            FROM face_embeddings
            WHERE topic_id = %s
            ORDER BY quality_score DESC
        """, (identity_id,))
        embeddings = cursor.fetchall()
        
        return identity, embeddings


# ============================================
# Dashboard Views
# ============================================

def render_sidebar():
    """Renders the sidebar navigation."""
    st.sidebar.title("🎯 Navigation")
    
    pages = {
        "📊 Dashboard": "dashboard",
        "👥 Identity Gallery": "gallery",
        "🔍 Face Search": "search",
        "⚙️ Settings": "settings"
    }
    
    # Use session state for navigation
    if 'page' not in st.session_state:
        st.session_state.page = "dashboard"
    
    for label, page_id in pages.items():
        if st.sidebar.button(label, key=f"nav_{page_id}", use_container_width=True):
            st.session_state.page = page_id
            st.rerun()
    
    st.sidebar.divider()
    
    # Health status indicator
    health = get_health_status()
    if health:
        st.sidebar.subheader("System Health")
        
        status_items = [
            ("Database", health['database']),
            ("Telegram", health['telegram']),
            ("Face Model", health['face_model']),
            ("Hub Access", health['hub_access'])
        ]
        
        for name, ok in status_items:
            icon = "✅" if ok else "❌"
            st.sidebar.text(f"{icon} {name}")
        
        if health['check_time']:
            st.sidebar.caption(f"Last check: {health['check_time'].strftime('%H:%M:%S')}")


def render_dashboard():
    """Renders the main dashboard view."""
    st.title("📊 Face Recognition Dashboard")
    
    # Statistics row
    stats = get_statistics()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("👤 Identities", stats['identities'])
    
    with col2:
        st.metric("🧬 Embeddings", stats['embeddings'])
    
    with col3:
        st.metric("📨 Messages", stats['messages'])
    
    with col4:
        st.metric("📱 Accounts", stats['accounts'])
    
    # Second row
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("🖼️ Processed Media", stats['processed_media'])
    
    with col2:
        st.metric("📈 Last 24h", stats['recent_processed'])
    
    st.divider()
    
    # Activity chart
    st.subheader("📈 Processing Activity (Last 24 Hours)")
    activity = get_recent_activity()
    
    if not activity.empty:
        st.bar_chart(activity.set_index('hour')['count'])
    else:
        st.info("No activity recorded in the last 24 hours.")
    
    st.divider()
    
    # Scanning progress
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔄 Scanning Progress")
        progress = get_scan_progress()
        
        if not progress.empty:
            # Add progress percentage
            progress['Progress'] = progress.apply(
                lambda r: f"{(r['Processed'] / r['Total'] * 100):.1f}%" if r['Total'] > 0 else "0%",
                axis=1
            )
            st.dataframe(progress[['Chat', 'Progress', 'Mode', 'Last Updated']], hide_index=True)
        else:
            st.info("No chats being scanned.")
    
    with col2:
        st.subheader("⚠️ Recent Errors")
        errors = get_recent_errors()
        
        if not errors.empty:
            st.dataframe(errors, hide_index=True)
        else:
            st.success("No recent errors!")


def render_gallery():
    """Renders the identity gallery view."""
    st.title("👥 Identity Gallery")
    
    # Controls
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search = st.text_input("🔍 Search by name or ID", key="gallery_search")
    
    with col2:
        sort_by = st.selectbox("Sort by", ["last_seen", "first_seen", "name", "faces"], key="gallery_sort")
    
    with col3:
        page = st.number_input("Page", min_value=1, value=1, key="gallery_page")
    
    # Fetch identities
    identities, total = get_identities(page, 12, search, sort_by)
    
    total_pages = (total + 11) // 12
    st.caption(f"Showing page {page} of {total_pages} ({total} identities)")
    
    if not identities:
        st.info("No identities found.")
        return
    
    # Display grid (4 columns)
    cols = st.columns(4)
    
    for idx, identity in enumerate(identities):
        id_, topic_id, label, first_seen, last_seen, emb_count, msg_count, exemplar = identity
        
        with cols[idx % 4]:
            with st.container():
                st.markdown(f"""
                <div class="identity-card">
                    <h4>{label}</h4>
                    <p>🧬 {emb_count} faces | 📨 {msg_count} messages</p>
                    <p>Last seen: {last_seen.strftime('%Y-%m-%d') if last_seen else 'N/A'}</p>
                </div>
                """, unsafe_allow_html=True)
                
                if st.button("View Details", key=f"view_{id_}"):
                    st.session_state.selected_identity = id_
                    st.session_state.page = "detail"
                    st.rerun()


def render_identity_detail():
    """Renders detailed view for a single identity."""
    if 'selected_identity' not in st.session_state:
        st.warning("No identity selected.")
        if st.button("← Back to Gallery"):
            st.session_state.page = "gallery"
            st.rerun()
        return
    
    identity_id = st.session_state.selected_identity
    identity, embeddings = get_identity_detail(identity_id)
    
    if not identity:
        st.error("Identity not found.")
        return
    
    id_, topic_id, label, first_seen, last_seen, emb_count, msg_count, exemplar = identity
    
    # Header
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.title(f"👤 {label}")
    
    with col2:
        if st.button("← Back to Gallery"):
            st.session_state.page = "gallery"
            st.rerun()
    
    # Stats
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Topic ID", topic_id)
    with col2:
        st.metric("Faces", emb_count)
    with col3:
        st.metric("Messages", msg_count)
    with col4:
        st.metric("First Seen", first_seen.strftime('%Y-%m-%d') if first_seen else "N/A")
    
    st.divider()
    
    # Rename form
    st.subheader("✏️ Rename Identity")
    new_name = st.text_input("New name", value=label, key="rename_input")
    
    if st.button("Rename", key="rename_btn"):
        if new_name and new_name != label:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE telegram_topics SET label = %s WHERE id = %s",
                    (new_name, identity_id)
                )
                conn.commit()
            st.success(f"Renamed to: {new_name}")
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    
    # Face embeddings table
    st.subheader(f"🧬 Face Embeddings ({len(embeddings)})")
    
    if embeddings:
        emb_df = pd.DataFrame(embeddings, columns=[
            'ID', 'Chat ID', 'Message ID', 'Quality', 'Detected At'
        ])
        emb_df['Quality'] = emb_df['Quality'].apply(lambda x: f"{x:.2f}")
        st.dataframe(emb_df, hide_index=True)
    else:
        st.info("No embeddings found for this identity.")
    
    st.divider()
    
    # Merge form
    st.subheader("🔗 Merge with Another Identity")
    
    # Get other identities for merge target
    other_identities, _ = get_identities(1, 100, "", "name")
    other_options = {f"{i[2]} (ID: {i[0]})": i[0] for i in other_identities if i[0] != identity_id}
    
    if other_options:
        target = st.selectbox("Merge into", list(other_options.keys()), key="merge_target")
        
        if st.button("Merge", key="merge_btn", type="primary"):
            target_id = other_options[target]
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Move all embeddings to target
                cursor.execute(
                    "UPDATE face_embeddings SET topic_id = %s WHERE topic_id = %s",
                    (target_id, identity_id)
                )
                
                # Move all uploaded media to target
                cursor.execute(
                    "UPDATE uploaded_media SET topic_id = %s WHERE topic_id = %s",
                    (target_id, identity_id)
                )
                
                # Delete source identity
                cursor.execute("DELETE FROM telegram_topics WHERE id = %s", (identity_id,))
                
                # Update target counts
                cursor.execute("""
                    UPDATE telegram_topics SET
                        embedding_count = (SELECT COUNT(*) FROM face_embeddings WHERE topic_id = %s),
                        message_count = (SELECT COUNT(*) FROM uploaded_media WHERE topic_id = %s)
                    WHERE id = %s
                """, (target_id, target_id, target_id))
                
                conn.commit()
            
            st.success(f"Merged into {target}")
            st.cache_data.clear()
            st.session_state.page = "gallery"
            st.rerun()
    else:
        st.info("No other identities available for merging.")


def render_face_search():
    """Renders the face search interface."""
    st.title("🔍 Face Search")
    st.caption("Upload an image to find matching identities")
    
    uploaded_file = st.file_uploader(
        "Upload an image",
        type=['jpg', 'jpeg', 'png', 'webp'],
        key="face_search_upload"
    )
    
    if uploaded_file:
        st.image(uploaded_file, caption="Uploaded Image", width=300)
        
        if st.button("Search", type="primary"):
            with st.spinner("Processing..."):
                try:
                    # Import face processor
                    from face_processor import FaceProcessor
                    import io
                    
                    processor = FaceProcessor.get_instance()
                    
                    # Process image
                    image_bytes = io.BytesIO(uploaded_file.getvalue())
                    import asyncio
                    faces = asyncio.run(processor.process_image(image_bytes))
                    
                    if not faces:
                        st.warning("No faces detected in the uploaded image.")
                        return
                    
                    st.success(f"Detected {len(faces)} face(s)")
                    
                    # Search for each face
                    for idx, face in enumerate(faces):
                        st.subheader(f"Face {idx + 1}")
                        
                        embedding = face['embedding']
                        quality = face['quality']
                        
                        st.caption(f"Quality: {quality:.2f}")
                        
                        # Search database
                        from identity_matcher import IdentityMatcher
                        
                        with get_db_connection() as conn:
                            cursor = conn.cursor()
                            
                            # Use pgvector for similarity search
                            cursor.execute("""
                                SELECT 
                                    t.id, t.label, t.embedding_count,
                                    1 - (e.embedding <=> %s::vector) as similarity
                                FROM face_embeddings e
                                JOIN telegram_topics t ON e.topic_id = t.id
                                ORDER BY e.embedding <=> %s::vector
                                LIMIT 5
                            """, (embedding.tolist(), embedding.tolist()))
                            
                            matches = cursor.fetchall()
                        
                        if matches:
                            for match in matches:
                                id_, label, count, similarity = match
                                
                                if similarity > 0.55:  # Threshold
                                    col1, col2, col3 = st.columns([2, 1, 1])
                                    
                                    with col1:
                                        st.write(f"**{label}**")
                                    with col2:
                                        st.write(f"Similarity: {similarity:.2%}")
                                    with col3:
                                        if st.button("View", key=f"search_view_{id_}_{idx}"):
                                            st.session_state.selected_identity = id_
                                            st.session_state.page = "detail"
                                            st.rerun()
                        else:
                            st.info("No matching identities found.")
                
                except ImportError:
                    st.error("Face processor not available. Make sure InsightFace is installed.")
                except Exception as e:
                    st.error(f"Error processing image: {e}")


def render_settings():
    """Renders the settings page."""
    st.title("⚙️ Settings")
    
    st.subheader("System Information")
    
    # Environment info
    col1, col2 = st.columns(2)
    
    with col1:
        st.text(f"Hub Group ID: {os.getenv('HUB_GROUP_ID', 'Not set')}")
        st.text(f"Database: {os.getenv('DB_NAME', 'Not set')}")
    
    with col2:
        st.text(f"Similarity Threshold: {os.getenv('SIMILARITY_THRESHOLD', '0.55')}")
        st.text(f"GPU Enabled: {os.getenv('USE_GPU', 'false')}")
    
    st.divider()
    
    # Cache controls
    st.subheader("Cache Management")
    
    if st.button("Clear Dashboard Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")
    
    st.divider()
    
    # Health check history
    st.subheader("Recent Health Checks")
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT check_time, database_ok, telegram_ok, 
                   face_model_ok, hub_access_ok, queue_size
            FROM health_checks
            ORDER BY check_time DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
    
    if rows:
        df = pd.DataFrame(rows, columns=[
            'Time', 'Database', 'Telegram', 'Face Model', 'Hub', 'Queue'
        ])
        st.dataframe(df, hide_index=True)
    else:
        st.info("No health check history.")


# ============================================
# Main Application
# ============================================

def main():
    """Main application entry point."""
    render_sidebar()
    
    page = st.session_state.get('page', 'dashboard')
    
    if page == 'dashboard':
        render_dashboard()
    elif page == 'gallery':
        render_gallery()
    elif page == 'detail':
        render_identity_detail()
    elif page == 'search':
        render_face_search()
    elif page == 'settings':
        render_settings()
    else:
        render_dashboard()


if __name__ == "__main__":
    main()
