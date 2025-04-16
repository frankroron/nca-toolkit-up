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
            # Configure yt-dlp options - focusing on reliability
            ydl_opts = {
                'format': 'bestvideo+bestaudio/best',
                'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),  # Use title for more reliable file naming
                'restrictfilenames': True,  # Restrict filenames to ASCII chars to avoid issues
                'noplaylist': True,  # Only download single video, not playlist
                'merge_output_format': 'mp4',
                'postprocessors': [],  # We'll manually handle the file instead
                'quiet': False,  # Enable output for debugging
                'no_warnings': False,  # Enable warnings for debugging
                'verbose': True  # Add verbose output for debugging
            }


            # Add format options if specified
            if format_options:
                # If a quality string is provided directly, use it as-is
                if format_options.get('quality'):
                    ydl_opts['format'] = format_options['quality']
                else:
                    # Otherwise, build the format string from components
                    format_str = []
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
                
                # Log the final format string for debugging
                logger.info(f"Job {job_id}: Using format string: {ydl_opts.get('format')}")

            # Audio extraction will be handled manually after download
            extract_audio = False
            audio_format = 'mp3'
            audio_quality = '192'
            
            if audio_options:
                if audio_options.get('extract'):
                    extract_audio = True
                    if audio_options.get('format'):
                        audio_format = audio_options['format']
                    if audio_options.get('quality'):
                        audio_quality = audio_options['quality']
                    
                    logger.info(f"Job {job_id}: Will extract audio after download: format={audio_format}, quality={audio_quality}")

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

            # Download the media
            logger.info(f"Job {job_id}: Starting download with options: {ydl_opts}")
            try:
                # Log all files in temp directory before download
                logger.info(f"Job {job_id}: Files in temp directory before download: {os.listdir(temp_dir)}")
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(media_url, download=True)
                    filename = info.get('_filename')
                    logger.info(f"Job {job_id}: Download completed, reported filename: {filename}")
                    
                    # Enhanced info logging
                    logger.info(f"Job {job_id}: Video info: format={info.get('format')}, "
                               f"format_id={info.get('format_id')}, "
                               f"ext={info.get('ext')}, "
                               f"acodec={info.get('acodec')}, "
                               f"vcodec={info.get('vcodec')}")
                
                # Log all files in temp directory after download
                logger.info(f"Job {job_id}: Files in temp directory after download: {os.listdir(temp_dir)}")
            except Exception as e:
                logger.error(f"Job {job_id}: Error during download: {str(e)}", exc_info=True)
                raise

                # Find the downloaded file(s) and handle them
                temp_files = os.listdir(temp_dir)
                logger.info(f"Job {job_id}: All files after download: {temp_files}")
                
                # Try to find any video file (prioritize mp4)
                mp4_files = [f for f in temp_files if f.endswith('.mp4')]
                mkv_files = [f for f in temp_files if f.endswith('.mkv')]
                webm_files = [f for f in temp_files if f.endswith('.webm')]
                video_files = [f for f in temp_files if f.endswith(('.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv'))]
                
                if mp4_files:
                    filename = os.path.join(temp_dir, mp4_files[0])
                elif mkv_files:
                    filename = os.path.join(temp_dir, mkv_files[0])
                elif webm_files:
                    filename = os.path.join(temp_dir, webm_files[0])
                elif video_files:
                    filename = os.path.join(temp_dir, video_files[0])
                else:
                    # If no video files found, take any file
                    if temp_files:
                        filename = os.path.join(temp_dir, temp_files[0])
                    else:
                        filename = None
                        
                logger.info(f"Job {job_id}: Selected file: {filename}")

                if not filename or not os.path.exists(filename):
                    # List all files in the temp directory for debugging
                    temp_files = os.listdir(temp_dir)
                    logger.error(f"Job {job_id}: Expected media file not found. Files in {temp_dir}: {temp_files}")
                    raise FileNotFoundError(f"Expected media file not found in {temp_dir}")
                else:
                    # Check if the file has content
                    file_size = os.path.getsize(filename)
                    logger.info(f"Job {job_id}: Found media file: {filename}, size: {file_size} bytes")
                    
                    if file_size == 0:
                        logger.error(f"Job {job_id}: File has zero size: {filename}")
                        raise ValueError(f"Downloaded file has zero size: {filename}")
                    
                    # For video files, we should convert to ensure audio is included
                    # This is a simple manual merge to ensure we have audio
                    if filename.endswith(('.mp4', '.mkv', '.webm', '.avi')) and file_size > 1000:
                        output_filename = os.path.join(temp_dir, 'output.mp4')
                        try:
                            import subprocess
                            logger.info(f"Job {job_id}: Converting file to ensure audio is included...")
                            # Use ffmpeg to convert file with audio and video tracks preserved
                            subprocess.check_call([
                                'ffmpeg', '-i', filename, 
                                '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental',
                                output_filename
                            ], stderr=subprocess.STDOUT)
                            
                            if os.path.exists(output_filename) and os.path.getsize(output_filename) > 1000:
                                old_filename = filename
                                filename = output_filename
                                logger.info(f"Job {job_id}: Successfully converted file to {filename}")
                                # Remove the original file
                                if old_filename != filename and os.path.exists(old_filename):
                                    os.remove(old_filename)
                        except Exception as e:
                            logger.error(f"Job {job_id}: Error converting file: {str(e)}")
                            # Continue with the original file if conversion fails
                
                # Process audio extraction if requested
                audio_url = None
                if extract_audio and filename and os.path.exists(filename):
                    try:
                        logger.info(f"Job {job_id}: Extracting audio from {filename}")
                        audio_output = os.path.join(temp_dir, f"audio.{audio_format}")
                        
                        # Use ffmpeg to extract audio
                        import subprocess
                        cmd = [
                            'ffmpeg', '-i', filename, 
                            '-vn',  # No video
                            '-acodec', 'libmp3lame' if audio_format == 'mp3' else audio_format,
                            '-ab', f"{audio_quality}k",  # Bitrate
                            '-ar', '44100',  # Sample rate
                            '-y',  # Overwrite output
                            audio_output
                        ]
                        logger.info(f"Job {job_id}: Running audio extraction: {' '.join(cmd)}")
                        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
                        
                        if os.path.exists(audio_output) and os.path.getsize(audio_output) > 0:
                            logger.info(f"Job {job_id}: Audio extraction successful: {audio_output}")
                            # Upload the audio file
                            audio_url = upload_file(audio_output)
                            logger.info(f"Job {job_id}: Audio file uploaded to {audio_url}")
                            # Remove the temporary audio file
                            os.remove(audio_output)
                    except Exception as e:
                        logger.error(f"Job {job_id}: Error extracting audio: {str(e)}", exc_info=True)
                
                # Upload to cloud storage
                cloud_url = upload_file(filename)
                logger.info(f"Job {job_id}: Video file uploaded to {cloud_url}")
                
                # Clean up the temporary file
                try:
                    os.remove(filename)
                except Exception as e:
                    logger.warning(f"Job {job_id}: Error removing temporary file: {str(e)}")

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
                        "format": audio_format,
                        "quality": audio_quality
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
        logger.error(f"Job {job_id}: Error during download process - {str(e)}")
        return str(e), "/v1/media/download", 500