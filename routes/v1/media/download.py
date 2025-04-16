from flask import Blueprint
from app_utils import *
import logging
import os
import yt_dlp
import tempfile
from werkzeug.utils import secure_filename
import uuid
from services.cloud_storage import upload_file
from services.authentication import authenticate
from services.file_management import download_file
from urllib.parse import quote

v1_media_download_bp = Blueprint('v1_media_download', __name__)
logger = logging.getLogger(__name__)

@v1_media_download_bp.route('/v1/BETA/media/download', methods=['POST'])
@authenticate
@validate_payload({
    "type": "object",
    "properties": {
        "media_url": {"type": "string", "format": "uri"},
        "webhook_url": {"type": "string", "format": "uri"},
        "id": {"type": "string"},
        "format": {
            "type": "object",
            "properties": {
                "quality": {"type": "string"},
                "format_id": {"type": "string"},
                "resolution": {"type": "string"},
                "video_codec": {"type": "string"},
                "audio_codec": {"type": "string"}
            }
        },
        "audio": {
            "type": "object",
            "properties": {
                "extract": {"type": "boolean"},
                "format": {"type": "string"},
                "quality": {"type": "string"}
            }
        },
        "thumbnails": {
            "type": "object",
            "properties": {
                "download": {"type": "boolean"},
                "download_all": {"type": "boolean"},
                "formats": {"type": "array", "items": {"type": "string"}},
                "convert": {"type": "boolean"},
                "embed_in_audio": {"type": "boolean"}
            }
        },
        "subtitles": {
            "type": "object",
            "properties": {
                "download": {"type": "boolean"},
                "languages": {"type": "array", "items": {"type": "string"}},
                "formats": {"type": "array", "items": {"type": "string"}}
            }
        },
        "download": {
            "type": "object",
            "properties": {
                "max_filesize": {"type": "integer"},
                "rate_limit": {"type": "string"},
                "retries": {"type": "integer"}
            }
        }
    },
    "required": ["media_url"],
    "additionalProperties": False
})
@queue_task_wrapper(bypass_queue=False)
def download_media(job_id, data):
    media_url = data['media_url']

    format_options = data.get('format', {})
    audio_options = data.get('audio', {})
    thumbnail_options = data.get('thumbnails', {})
    subtitle_options = data.get('subtitles', {})
    download_options = data.get('download', {})

    logger.info(f"Job {job_id}: Received download request for {media_url}")

    try:
        # Create a temporary directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            # Configure yt-dlp options
            ydl_opts = {
                'format': 'bestvideo+bestaudio/best',
                'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
                'merge_output_format': 'mp4',
                'quiet': False,  # Enable output for debugging
                'no_warnings': False,  # Show warnings for debugging
                'verbose': True,  # More verbose output
                'progress': True,  # Show progress
                'prefer_ffmpeg': True,  # Prefer ffmpeg for processing
                'writethumbnail': thumbnail_options.get('download', False),  # Add thumbnail downloading here
                'writeinfojson': True,  # Write info json for debugging
                'paths': {'temp': temp_dir, 'home': temp_dir},  # Ensure all paths are in our temp directory
                'nocheckcertificate': True,  # Skip HTTPS certificate validation for problematic sites
                'ignoreerrors': False,  # Don't ignore errors during download
                'logtostderr': True,  # Log to stderr for debugging
            }
            
            # Log the temporary directory for debugging
            logger.info(f"Using temporary directory: {temp_dir}")


            # Add format options if specified
            if format_options:
                format_str = []
                if format_options.get('quality'):
                    format_str.append(format_options['quality'])
                if format_options.get('format_id'):
                    format_str.append(format_options['format_id'])
                if format_options.get('resolution'):
                    format_str.append(format_options['resolution'])
                if format_options.get('video_codec'):
                    format_str.append(format_options['video_codec'])
                if format_options.get('audio_codec'):
                    format_str.append(format_options['audio_codec'])
                if format_str:
                    ydl_opts['format'] = '+'.join(format_str)

            # Add audio options if specified
            if audio_options and audio_options.get('extract'):
                # Set up audio extraction via postprocessor
                audio_format = audio_options.get('format', 'mp3')
                audio_quality = audio_options.get('quality', '192')
                
                # Add audio extraction postprocessor
                if 'postprocessors' not in ydl_opts:
                    ydl_opts['postprocessors'] = []
                
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': audio_format,
                    'preferredquality': audio_quality,
                })
                
                # Important: keep video if we're extracting audio
                ydl_opts['keepvideo'] = True

            # Add thumbnail options if specified
            if thumbnail_options:
                ydl_opts['writesubtitles'] = thumbnail_options.get('download', False)
                ydl_opts['writeallsubtitles'] = thumbnail_options.get('download_all', False)
                if thumbnail_options.get('formats'):
                    ydl_opts['subtitleslangs'] = thumbnail_options['formats']
                ydl_opts['convert_thumbnails'] = thumbnail_options.get('convert', False)
                ydl_opts['embed_thumbnail_in_audio'] = thumbnail_options.get('embed_in_audio', False)

            # Add subtitle options if specified
            if subtitle_options:
                ydl_opts['writesubtitles'] = subtitle_options.get('download', False)
                if subtitle_options.get('languages'):
                    ydl_opts['subtitleslangs'] = subtitle_options['languages']
                if subtitle_options.get('formats'):
                    ydl_opts['subtitlesformat'] = subtitle_options['formats']

            # Add download options if specified
            if download_options:
                if download_options.get('max_filesize'):
                    ydl_opts['max_filesize'] = download_options['max_filesize']
                if download_options.get('rate_limit'):
                    ydl_opts['limit_rate'] = download_options['rate_limit']
                if download_options.get('retries'):
                    ydl_opts['retries'] = download_options['retries']

            # Configure postprocessors
            if 'postprocessors' not in ydl_opts:
                ydl_opts['postprocessors'] = []
            
            # Add FFmpeg merger postprocessor if we're not just extracting audio
            if not (audio_options and audio_options.get('extract') and not ydl_opts.get('keepvideo')):
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegMerger',
                    'ffmpeg_location': None,  # Let yt-dlp find ffmpeg automatically
                })

            # Download the media
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(media_url, download=True)
                
                # Get correct filename directly from info dict
                video_id = info.get('id')
                extension = info.get('ext', 'mp4')
                expected_filename = os.path.join(temp_dir, f"{video_id}.{extension}")
                
                # Log all files in temp directory for debugging
                logger.info(f"Files in temp directory {temp_dir}: {os.listdir(temp_dir)}")
                
                # Check if file exists directly with expected name pattern
                if os.path.exists(expected_filename):
                    filename = expected_filename
                else:
                    # Try format specific filenames (common with yt-dlp)
                    format_id = info.get('format_id', 'f0')
                    format_filename = os.path.join(temp_dir, f"{video_id}.{format_id}.{extension}")
                    if os.path.exists(format_filename):
                        filename = format_filename
                    # Fall back to searching for any file with the video_id in the name
                    else:
                        found = False
                        for f in os.listdir(temp_dir):
                            full_path = os.path.join(temp_dir, f)
                            if os.path.isfile(full_path) and video_id in f:
                                # Handle .part files if needed
                                if f.endswith('.part'):
                                    try:
                                        new_path = full_path[:-5]  # Remove ".part"
                                        os.rename(full_path, new_path)
                                        filename = new_path
                                    except:
                                        filename = full_path  # If rename fails, use as is
                                else:
                                    filename = full_path
                                found = True
                                break
                        
                        # If still not found, look for any media file
                        if not found:
                            media_extensions = ['.mp4', '.mkv', '.webm', '.mp3', '.m4a', '.wav']
                            for f in os.listdir(temp_dir):
                                full_path = os.path.join(temp_dir, f)
                                if os.path.isfile(full_path) and any(f.endswith(ext) for ext in media_extensions):
                                    filename = full_path
                                    found = True
                                    break
                        
                        if not found:
                            raise FileNotFoundError(f"Expected media file not found in {temp_dir}. Directory contents: {os.listdir(temp_dir)}")
                
                logger.info(f"Using file: {filename}")
                
                # Verify file exists and has size
                if not os.path.exists(filename):
                    raise FileNotFoundError(f"File {filename} does not exist")
                
                file_size = os.path.getsize(filename)
                if file_size == 0:
                    raise ValueError(f"File {filename} exists but has zero size")
                
                logger.info(f"File size: {file_size} bytes")

                
                # Verify file exists and has content before upload
                if not os.path.exists(filename):
                    raise FileNotFoundError(f"File {filename} does not exist before upload")
                
                file_size = os.path.getsize(filename)
                if file_size == 0:
                    raise ValueError(f"File {filename} has zero size, cannot upload empty file")
                
                logger.info(f"Uploading file {filename} ({file_size} bytes) to cloud storage")
                
                try:
                    # Upload to cloud storage
                    cloud_url = upload_file(filename)
                    logger.info(f"Upload successful: {cloud_url}")
                    
                    # Clean up the temporary file only after successful upload
                    os.remove(filename)
                except Exception as e:
                    logger.error(f"Upload failed: {str(e)}")
                    # If we have the info json, include it in the error
                    info_json_path = os.path.join(temp_dir, f"{info.get('id')}.info.json")
                    if os.path.exists(info_json_path):
                        with open(info_json_path, 'r') as f:
                            logger.error(f"Info JSON contents: {f.read()}")
                    raise RuntimeError(f"Failed to upload file {filename}: {str(e)}")

                # Check for audio file if extraction was requested
                audio_url = None
                if audio_options and audio_options.get('extract'):
                    audio_format = audio_options.get('format', 'mp3')
                    video_id = info.get('id')
                    
                    # Try different possible naming patterns for the audio file
                    possible_audio_names = [
                        f"{video_id}.{audio_format}",
                        f"{video_id}.f*.{audio_format}",  # Format-specific pattern
                        f"{os.path.splitext(os.path.basename(filename))[0]}.{audio_format}"  # Based on video filename
                    ]
                    
                    logger.info(f"Looking for audio files with patterns: {possible_audio_names}")
                    logger.info(f"Files in directory: {os.listdir(temp_dir)}")
                    
                    audio_path = None
                    # First look for exact matches
                    for pattern in possible_audio_names:
                        if '*' not in pattern:  # Exact filename
                            potential_path = os.path.join(temp_dir, pattern)
                            if os.path.exists(potential_path):
                                audio_path = potential_path
                                break
                    
                    # If not found, try pattern matching
                    if not audio_path:
                        for f in os.listdir(temp_dir):
                            if f.endswith(f'.{audio_format}') and os.path.join(temp_dir, f) != filename:
                                audio_path = os.path.join(temp_dir, f)
                                break
                    
                    if audio_path:
                        logger.info(f"Found audio file: {audio_path}")
                        try:
                            # Upload audio file
                            audio_url = upload_file(audio_path)
                            # Clean up
                            os.remove(audio_path)
                        except Exception as e:
                            logger.error(f"Error uploading audio file: {str(e)}")
                    else:
                        logger.warning(f"No audio file found in {temp_dir} with format {audio_format}")

                # Prepare response
                response = {
                    "media": {
                        "media_url": cloud_url,
                        "title": info.get('title'),
                        "format_id": info.get('format_id'),
                        "ext": info.get('ext'),
                        "resolution": info.get('resolution'),
                        "filesize": info.get('filesize'),
                        "width": info.get('width'),
                        "height": info.get('height'),
                        "fps": info.get('fps'),
                        "video_codec": info.get('vcodec'),
                        "audio_codec": info.get('acodec'),
                        "upload_date": info.get('upload_date'),
                        "duration": info.get('duration'),
                        "view_count": info.get('view_count'),
                        "uploader": info.get('uploader'),
                        "uploader_id": info.get('uploader_id'),
                        "description": info.get('description')
                    }
                }

                # Add audio URL if it was extracted
                if audio_url:
                    response["audio"] = {
                        "audio_url": audio_url,
                        "format": audio_options.get('format', 'mp3'),
                        "quality": audio_options.get('quality', '192')
                    }

                # Add thumbnails if available and requested
                if info.get('thumbnails') and thumbnail_options.get('download', False):
                    response["thumbnails"] = []
                    for thumbnail in info['thumbnails']:
                        if thumbnail.get('url'):
                            try:
                                # Download the thumbnail first
                                thumbnail_path = download_file(thumbnail['url'], temp_dir)
                                # Upload to cloud storage
                                thumbnail_url = upload_file(thumbnail_path)
                                # Clean up the temporary thumbnail file
                                os.remove(thumbnail_path)
                                
                                response["thumbnails"].append({
                                    "id": thumbnail.get('id', 'default'),
                                    "image_url": thumbnail_url,
                                    "width": thumbnail.get('width'),
                                    "height": thumbnail.get('height'),
                                    "original_format": thumbnail.get('ext'),
                                    "converted": thumbnail.get('converted', False)
                                })
                            except Exception as e:
                                logger.error(f"Error processing thumbnail: {str(e)}")
                                continue
                
                return response, "/v1/media/download", 200

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Job {job_id}: Error during download process - {str(e)}")
        logger.error(f"Traceback: {error_trace}")
        
        # Return a more informative error message
        error_message = f"Download failed: {str(e)}. Please check the URL and try again."
        if "No such file or directory" in str(e):
            error_message = f"The system could not locate the downloaded file. This may be due to a yt-dlp extraction failure or an unsupported video format. Error: {str(e)}"
        
        return error_message, "/v1/media/download", 500