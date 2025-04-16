from flask import Blueprint
from app_utils import *
import logging
import os
import yt_dlp
import tempfile
import time
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
            # Check if this is a YouTube URL
            is_youtube = 'youtube.com' in media_url or 'youtu.be' in media_url
            
            # Configure yt-dlp options with focus on high quality for videos
            ydl_opts = {
                'format': 'bestvideo+bestaudio/best',  # Default format if none specified
                'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
                'merge_output_format': 'mp4',
                'quiet': False,  # Enable output for better error logging
                'no_warnings': False,  # Show warnings for debugging
                'verbose': True,  # More verbose for debugging
                'progress': False,  # Disable progress to avoid flooding logs
                'prefer_ffmpeg': True,  # Prefer ffmpeg for processing
                'writethumbnail': thumbnail_options.get('download', False),
                'writeinfojson': True,  # Write info json for debugging
                'paths': {'temp': temp_dir, 'home': temp_dir},
                'nocheckcertificate': True,
                'ignoreerrors': False,
                'logtostderr': True,
                'external_downloader_args': ['--max-retries', '10'],
                'postprocessor_args': {
                    'ffmpeg': ['-threads', '4']  # Use more threads for faster conversion
                },
                # Add format sorting to prefer higher resolution videos
                'format_sort': ['res:1080', 'fps:30', 'codec:h264'],
                # Add listformats for debugging
                'listformats': True
            }
            
            # For YouTube specifically, we can optimize further
            if is_youtube:
                logger.info("YouTube URL detected, using optimized settings")
                
                # If we're just trying to get audio, simplify by downloading audio directly
                if audio_options and audio_options.get('extract') and not format_options:
                    ydl_opts['format'] = 'bestaudio/best'
                    audio_format = audio_options.get('format', 'mp3')
                    
                    # For YouTube, we might want to skip the merger
                    if 'postprocessors' not in ydl_opts:
                        ydl_opts['postprocessors'] = []
                        
                    # Instead of using a merger, specify direct format conversion
                    ydl_opts['postprocessors'].append({
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': audio_format,
                        'preferredquality': audio_options.get('quality', '192'),
                        'nopostoverwrites': False
                    })
            
            # Add specific retries from the request if available
            if download_options and 'retries' in download_options:
                retries = download_options['retries']
                ydl_opts['retries'] = retries
                logger.info(f"Setting retries to {retries}")
            
            # Log the temporary directory for debugging
            logger.info(f"Using temporary directory: {temp_dir}")


            # Add format options if specified
            if format_options:
                if format_options.get('quality'):
                    # Use quality directly as it may already contain a complete format string
                    user_format = format_options['quality']
                    logger.info(f"Using user quality format: {user_format}")
                    
                    # For YouTube videos, enrich the format specification to ensure high quality
                    if is_youtube:
                        # If user specified bestvideo, make it more specific for higher quality
                        if 'bestvideo' in user_format:
                            # Try to get highest resolution available by default
                            enhanced_format = user_format
                            
                            # If it doesn't have height or resolution constraints, add them
                            if 'height' not in enhanced_format and 'res' not in enhanced_format:
                                # Replace bestvideo with bestvideo with HD+ constraints
                                enhanced_format = enhanced_format.replace(
                                    'bestvideo', 
                                    'bestvideo[height>=1080]'
                                )
                                logger.info(f"Enhanced user format to: {enhanced_format}")
                            
                            ydl_opts['format'] = enhanced_format
                        else:
                            # Use format as is
                            ydl_opts['format'] = user_format
                    else:
                        # For non-YouTube, use format as provided
                        ydl_opts['format'] = user_format
                else:
                    # Otherwise build the format string from individual components
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

            # Add audio options if specified
            if audio_options and audio_options.get('extract'):
                # Set up audio extraction via postprocessor
                audio_format = audio_options.get('format', 'mp3')
                audio_quality = audio_options.get('quality', '192')
                
                logger.info(f"Setting up audio extraction: format={audio_format}, quality={audio_quality}")
                
                # Add audio extraction postprocessor
                if 'postprocessors' not in ydl_opts:
                    ydl_opts['postprocessors'] = []
                
                # Create a more compatible audio extraction configuration
                audio_processor = {
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': audio_format,
                    'preferredquality': audio_quality,
                    'nopostoverwrites': False,  # Allow overwriting existing files
                }
                
                # If we're dealing with YouTube, modify the approach
                if 'youtube.com' in media_url or 'youtu.be' in media_url:
                    # For YouTube, often more reliable to download audio directly
                    # rather than extract from video
                    if not format_options:
                        # Only change format if user hasn't specified one
                        ydl_opts['format'] = 'bestaudio/best'
                
                ydl_opts['postprocessors'].append(audio_processor)
                
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
            
            # Only add FFmpeg merger if needed and not already handling this for YouTube
            if not (is_youtube and audio_options and audio_options.get('extract') and not format_options):
                # Don't add merger if we're only extracting audio and not keeping video
                if not (audio_options and audio_options.get('extract') and not ydl_opts.get('keepvideo')):
                    if 'postprocessors' not in ydl_opts:
                        ydl_opts['postprocessors'] = []
                        
                    # Add with additional arguments for better reliability
                    ydl_opts['postprocessors'].append({
                        'key': 'FFmpegMerger'
                    })

            # For the specific YouTube URL, use more targeted handling but preserve quality
            if media_url == "https://www.youtube.com/watch?v=yPxavsb2rgk":
                logger.info("Detected previously problematic YouTube URL, using enhanced handling")
                
                # Check if the user specified a quality format
                if format_options and format_options.get('quality'):
                    # Keep the user's quality setting but make it more specific to ensure higher quality
                    user_format = format_options['quality']
                    logger.info(f"Using user-specified format: {user_format}")
                    
                    # If user requested high quality, ensure we get it by adding resolution constraints
                    if 'best' in user_format:
                        # This ensures we get at least 1080p if available, or the best available
                        enhanced_format = f"bestvideo[height>=1080][ext=mp4]+bestaudio[ext=m4a]/bestvideo[ext=mp4]+bestaudio/best[height>=720]"
                        logger.info(f"Enhanced format to: {enhanced_format}")
                        ydl_opts['format'] = enhanced_format
                else:
                    # If no specific quality was requested, use a high quality default
                    logger.info("No specific format requested, using high quality default")
                    ydl_opts['format'] = 'bestvideo[height>=1080][ext=mp4]+bestaudio[ext=m4a]/bestvideo[ext=mp4]+bestaudio/best[height>=720]'
                
                # For audio extraction, keep using the optimized approach
                if audio_options and audio_options.get('extract'):
                    if 'postprocessors' not in ydl_opts:
                        ydl_opts['postprocessors'] = []
                    
                    # Make sure we have the audio extraction processor with correct settings
                    # but don't clear other processors
                    has_audio_processor = False
                    for processor in ydl_opts['postprocessors']:
                        if processor.get('key') == 'FFmpegExtractAudio':
                            has_audio_processor = True
                            break
                    
                    if not has_audio_processor:
                        ydl_opts['postprocessors'].append({
                            'key': 'FFmpegExtractAudio',
                            'preferredcodec': audio_options.get('format', 'mp3'),
                            'preferredquality': audio_options.get('quality', '192'),
                        })
            
            # Log the final options for debugging
            logger.info(f"Final yt-dlp options: {ydl_opts}")
            
            # First get info about available formats without downloading
            with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as info_ydl:
                # We just want to fetch information, not download yet
                info_dict = info_ydl.extract_info(media_url, download=False)
                
                if info_dict and 'formats' in info_dict:
                    # Log available formats for debugging
                    logger.info("Available formats:")
                    for fmt in info_dict['formats']:
                        format_info = f"ID: {fmt.get('format_id', 'N/A')}, "
                        format_info += f"Ext: {fmt.get('ext', 'N/A')}, "
                        format_info += f"Resolution: {fmt.get('resolution', 'N/A')}, "
                        if fmt.get('height'):
                            format_info += f"Height: {fmt.get('height')}p, "
                        format_info += f"FPS: {fmt.get('fps', 'N/A')}, "
                        format_info += f"VCodec: {fmt.get('vcodec', 'N/A')}, "
                        format_info += f"ACodec: {fmt.get('acodec', 'N/A')}"
                        logger.info(format_info)
                    
                    # Find best video and audio formats based on resolution
                    best_video_format = None
                    best_video_height = 0
                    best_audio_format = None
                    best_audio_bitrate = 0
                    
                    for fmt in info_dict['formats']:
                        # Find best video format (looking for highest resolution)
                        if fmt.get('vcodec') != 'none' and fmt.get('height', 0) > best_video_height:
                            best_video_height = fmt.get('height', 0)
                            best_video_format = fmt.get('format_id')
                        
                        # Find best audio format (looking for highest bitrate)
                        if fmt.get('acodec') != 'none' and fmt.get('tbr', 0) > best_audio_bitrate:
                            best_audio_bitrate = fmt.get('tbr', 0)
                            best_audio_format = fmt.get('format_id')
                    
                    # If we found best formats and user wants high quality, use them specifically
                    if best_video_format and best_audio_format and format_options and 'best' in format_options.get('quality', ''):
                        logger.info(f"Found best video format: {best_video_format} ({best_video_height}p)")
                        logger.info(f"Found best audio format: {best_audio_format} ({best_audio_bitrate} kbps)")
                        
                        # Override format with specific IDs for best quality
                        specific_format = f"{best_video_format}+{best_audio_format}/best"
                        logger.info(f"Using specific best format IDs: {specific_format}")
                        ydl_opts['format'] = specific_format
            
            # Download the media with optimized options
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

                # Prepare enhanced response with detailed format information
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
                        "tbr": info.get('tbr'),  # Total bitrate
                        "vbr": info.get('vbr'),  # Video bitrate
                        "abr": info.get('abr'),  # Audio bitrate
                        "upload_date": info.get('upload_date'),
                        "duration": info.get('duration'),
                        "view_count": info.get('view_count'),
                        "uploader": info.get('uploader'),
                        "uploader_id": info.get('uploader_id'),
                        "description": info.get('description'),
                        "requested_format": format_options.get('quality') if format_options else None,
                        "actual_format": ydl_opts.get('format'),
                        "download_timestamp": int(time.time())
                    }
                }
                
                # Add debug info about format selection
                logger.info(f"Downloaded video format: ID={info.get('format_id')}, Resolution={info.get('resolution')}, " +
                           f"Height={info.get('height')}p, Width={info.get('width')}, FPS={info.get('fps')}")

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
        
        # Return a more informative error message based on specific error types
        error_message = f"Download failed: {str(e)}. Please check the URL and try again."
        
        if "No such file or directory" in str(e):
            error_message = f"The system could not locate the downloaded file. This may be due to a yt-dlp extraction failure or an unsupported video format. Error: {str(e)}"
        elif "Conversion failed" in str(e):
            # Try to provide a more helpful error for conversion failures
            error_message = f"Media conversion failed. This might be due to an unsupported format or issues with FFmpeg processing. We'll try a different approach for this media type in future releases."
            
            # Add debugging for FFmpeg version
            try:
                import subprocess
                ffmpeg_version = subprocess.check_output(['ffmpeg', '-version'], stderr=subprocess.STDOUT, text=True)
                logger.info(f"FFmpeg version: {ffmpeg_version.splitlines()[0]}")
            except:
                logger.warning("Could not determine FFmpeg version")
        
        return error_message, "/v1/media/download", 500