from flask import Blueprint, current_app, jsonify
from app_utils import *
import logging
import os
import yt_dlp
import tempfile
import time
import traceback
import shutil
import sys
import json
from werkzeug.utils import secure_filename
import uuid
from services.cloud_storage import upload_file
from services.authentication import authenticate
from services.file_management import download_file
from urllib.parse import quote

v1_media_download_bp = Blueprint('v1_media_download', __name__)
logger = logging.getLogger(__name__)

# Define a global error handler for this blueprint
@v1_media_download_bp.errorhandler(Exception)
def handle_error(error):
    logger.error(f"Unhandled exception in media download: {str(error)}")
    logger.error(traceback.format_exc())
    return jsonify({"error": "Media download failed", "message": str(error)}), 500

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
    """
    Completely rewritten function that handles media downloads in a more robust way.
    Uses a simpler approach and better error handling.
    """
    media_url = data['media_url']
    format_options = data.get('format', {})
    audio_options = data.get('audio', {})
    extract_audio = audio_options.get('extract', False)
    
    logger.info(f"Job {job_id}: Received download request for {media_url}")
    
    # Create a more persistent temporary directory
    # We'll manually clean it up to ensure it doesn't disappear during processing
    temp_dir = os.path.join(tempfile.gettempdir(), f"media_download_{job_id}_{uuid.uuid4().hex}")
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        # STEP 1: Determine if we're downloading a video or just audio
        download_audio_only = extract_audio and not format_options
        
        # STEP 2: Configure the simplest possible options for maximum reliability
        ydl_opts = {
            'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
            'quiet': False,
            'no_warnings': False,
            'ignoreerrors': False,
            'verbose': True,
            'writeinfojson': True,
            'paths': {'temp': temp_dir, 'home': temp_dir},
            'retries': 5,
            'fragment_retries': 5,
            'skip_unavailable_fragments': True,
            'keepvideo': extract_audio,  # Keep video if extracting audio
        }
        
        # STEP 3: Set the format based on what we're doing
        if download_audio_only:
            logger.info("Downloading audio only")
            ydl_opts['format'] = 'bestaudio/best'
            audio_format = audio_options.get('format', 'mp3')
            audio_quality = audio_options.get('quality', '192')
            
            # Add audio extraction with explicit configuration
            ydl_opts['postprocessors'] = []
            
            # For mp3, use very explicit configuration
            if audio_format == 'mp3':
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': audio_quality,
                })
                
                # Add detailed FFmpeg arguments for MP3 extraction
                ydl_opts['postprocessor_args'] = {
                    'FFmpegExtractAudio': [
                        # Force MP3 encoding with LAME
                        '-codec:a', 'libmp3lame',
                        # Set quality (lower is better for MP3)
                        '-q:a', audio_quality.replace('k', ''),
                        # No video
                        '-vn'
                    ]
                }
            else:
                # For other formats
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': audio_format,
                    'preferredquality': audio_quality,
                })
        elif format_options and format_options.get('quality'):
            logger.info(f"Using specified quality: {format_options.get('quality')}")
            ydl_opts['format'] = format_options.get('quality')
            
            # Always set merge_output_format to mp4 for combined formats
            # This ensures we get a proper mp4 container for the video
            if '+' in format_options.get('quality'):
                logger.info("Format contains a merge specification, setting merge_output_format")
                ydl_opts['merge_output_format'] = format_options.get('merge_output_format', 'mp4')
                
                # Add explicit option to ensure we include both video and audio
                logger.info("Setting explicit merge_output_format and ensuring audio is included")
                
                # For formats that specify bestvideo[ext=mp4]+bestaudio, ensure proper merge
                if 'bestvideo[ext=mp4]' in format_options.get('quality') and 'bestaudio' in format_options.get('quality'):
                    logger.info("Detected bestvideo+bestaudio format, ensuring proper mp4 output")
                    # Force FFmpeg to use the proper container format and ensure audio is included
                    ydl_opts['postprocessor_args'] = {
                        'ffmpeg': ['-c:v', 'copy', '-c:a', 'copy', '-f', 'mp4', '-movflags', 'faststart']
                    }
                
            # Handle both video download and audio extraction
            if extract_audio:
                logger.info("Adding audio extraction processor alongside video download")
                audio_format = audio_options.get('format', 'mp3')
                audio_quality = audio_options.get('quality', '192')
                
                # Need to ensure we're keeping the video
                ydl_opts['keepvideo'] = True
                
                # Add audio extraction postprocessor
                if 'postprocessors' not in ydl_opts:
                    ydl_opts['postprocessors'] = []

                # For mp3 format, we need to be more explicit about the codec
                if audio_format == 'mp3':
                    logger.info("Setting up explicit MP3 extraction with libmp3lame")
                    ydl_opts['postprocessors'].append({
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': audio_quality,
                        'nopostoverwrites': False
                    })
                    
                    # Specify MP3 encoding parameters explicitly
                    if 'postprocessor_args' not in ydl_opts:
                        ydl_opts['postprocessor_args'] = {}
                    
                    # Configure FFmpegExtractAudio to only affect the extracted audio, not the video file
                    ydl_opts['postprocessor_args']['FFmpegExtractAudio'] = [
                        '-codec:a', 'libmp3lame', 
                        '-q:a', audio_quality.replace('k', ''),
                        '-vn'  # No video in the audio file only
                    ]
                    
                    # Ensure we're making a copy of the video file with audio intact
                    if 'ffmpeg' not in ydl_opts['postprocessor_args']:
                        ydl_opts['postprocessor_args']['ffmpeg'] = ['-c:v', 'copy', '-c:a', 'copy', '-f', 'mp4', '-movflags', 'faststart']
                else:
                    # For other formats
                    ydl_opts['postprocessors'].append({
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': audio_format,
                        'preferredquality': audio_quality,
                        'nopostoverwrites': False
                    })
        else:
            logger.info("Using best video+audio format")
            ydl_opts['format'] = 'bestvideo+bestaudio/best'
            ydl_opts['merge_output_format'] = 'mp4'
            
            # Handle audio extraction if requested
            if extract_audio:
                logger.info("Adding audio extraction with default video")
                audio_format = audio_options.get('format', 'mp3')
                audio_quality = audio_options.get('quality', '192')
                
                # Need to ensure we're keeping the video
                ydl_opts['keepvideo'] = True
                
                # Add audio extraction postprocessor
                if 'postprocessors' not in ydl_opts:
                    ydl_opts['postprocessors'] = []
                    
                ydl_opts['postprocessors'].append({
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': audio_format,
                    'preferredquality': audio_quality,
                    'nopostoverwrites': False
                })
        
        # STEP 4: Create hooks to monitor the download and postprocessing
        downloaded_files = []
        
        def download_hook(d):
            if d['status'] == 'finished':
                logger.info(f"Download finished: {d['filename']}")
                if d['filename'] not in downloaded_files:
                    downloaded_files.append(d['filename'])
                
        def postprocess_hook(d):
            if d.get('status') == 'finished':
                logger.info(f"Postprocessing finished: {d}")
                
                # Check all possible file path locations in the dictionary
                if 'destination' in d and d['destination']:
                    filepath = d['destination']
                    logger.info(f"Postprocessed file destination: {filepath}")
                    if filepath not in downloaded_files:
                        downloaded_files.append(filepath)
                
                elif 'filepath' in d and d['filepath']:
                    filepath = d['filepath']
                    logger.info(f"Postprocessed file path: {filepath}")
                    if filepath not in downloaded_files:
                        downloaded_files.append(filepath)
                
                # Sometimes the filepath is nested in info_dict
                elif 'info_dict' in d and isinstance(d['info_dict'], dict):
                    info_dict = d['info_dict']
                    if 'filepath' in info_dict and info_dict['filepath']:
                        filepath = info_dict['filepath']
                        logger.info(f"Postprocessed file in info_dict: {filepath}")
                        if filepath not in downloaded_files:
                            downloaded_files.append(filepath)
                    
        ydl_opts['progress_hooks'] = [download_hook]
        ydl_opts['postprocessor_hooks'] = [postprocess_hook]
        
        # STEP 5: First attempt - standard download with yt-dlp
        logger.info("Starting download with yt-dlp...")
        info = None
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(media_url, download=True)
                
            if not info:
                raise ValueError("No information returned from yt-dlp")
                
            logger.info(f"Downloaded files via hook: {downloaded_files}")
            
            if not downloaded_files:
                # Check if files exist despite hook not catching them
                files = os.listdir(temp_dir)
                media_files = [f for f in files if not f.endswith('.info.json') and not f.endswith('.description')]
                
                if media_files:
                    logger.info(f"Files in directory: {media_files}")
                    downloaded_files = [os.path.join(temp_dir, f) for f in media_files]
                else:
                    raise FileNotFoundError("No files downloaded")
                    
        except Exception as e:
            logger.error(f"First download attempt failed: {str(e)}")
            
            # STEP 6: Fallback - simplified download with minimal options
            try:
                logger.info("Trying fallback download...")
                fallback_opts = {
                    'format': 'best',
                    'outtmpl': os.path.join(temp_dir, 'fallback.%(ext)s'),
                    'quiet': False,
                    'verbose': True,
                    'no_warnings': False,
                    'ignoreerrors': False,
                    'noplaylist': True,
                    'writeinfojson': True,
                }
                
                if extract_audio:
                    fallback_opts['postprocessors'] = [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': audio_options.get('format', 'mp3'),
                        'preferredquality': audio_options.get('quality', '192'),
                    }]
                
                with yt_dlp.YoutubeDL(fallback_opts) as ydl:
                    info = ydl.extract_info(media_url, download=True)
                
                # Check for downloaded files
                files = os.listdir(temp_dir)
                fallback_files = [f for f in files if f.startswith('fallback.') and not f.endswith('.info.json')]
                
                if fallback_files:
                    logger.info(f"Fallback files: {fallback_files}")
                    downloaded_files = [os.path.join(temp_dir, f) for f in fallback_files]
                else:
                    # Look for any other media files
                    media_files = [f for f in files if not f.endswith('.info.json') and not f.endswith('.description')]
                    
                    if media_files:
                        logger.info(f"Found other media files: {media_files}")
                        downloaded_files = [os.path.join(temp_dir, f) for f in media_files]
                    else:
                        raise FileNotFoundError("No files downloaded in fallback attempt")
                
            except Exception as fallback_error:
                logger.error(f"Fallback download failed: {str(fallback_error)}")
                
                # STEP 7: Final attempt - direct download with explicit filename
                try:
                    logger.info("Trying direct download via subprocess...")
                    import subprocess
                    final_output = os.path.join(temp_dir, "direct_download.mp4")
                    
                    cmd = [
                        'yt-dlp',
                        '--format', 'best',
                        '--output', final_output,
                        '--no-playlist',
                        media_url
                    ]
                    
                    logger.info(f"Executing command: {' '.join(cmd)}")
                    subprocess.run(cmd, check=True)
                    
                    if os.path.exists(final_output) and os.path.getsize(final_output) > 0:
                        logger.info(f"Direct download successful: {final_output}")
                        downloaded_files = [final_output]
                        
                        # Create minimal info
                        if not info:
                            info = {
                                'id': 'direct_download',
                                'title': os.path.basename(media_url),
                                'ext': 'mp4',
                                'format_id': 'best'
                            }
                    else:
                        raise FileNotFoundError(f"File not found or zero size after direct download")
                        
                except Exception as direct_error:
                    logger.error(f"Direct download failed: {str(direct_error)}")
                    raise RuntimeError(f"All download attempts failed")
        
        # STEP 8: Process files for upload
        if not downloaded_files:
            raise FileNotFoundError("No files were downloaded")
            
        logger.info(f"Processing {len(downloaded_files)} downloaded files")
        
        # STEP 9: Handle video and audio (if requested) - Improved selection logic
        video_file = None
        audio_file = None
        requested_audio_format = audio_options.get('format', 'mp3') if extract_audio else None
        
        logger.info(f"Looking for files with these criteria: video=mp4, audio={requested_audio_format}")
        logger.info(f"Available files: {downloaded_files}")
        
        # Before we select files, let's check if the MP4 video includes audio or not
        mp4_files = [f for f in downloaded_files if f.endswith('.mp4') and os.path.exists(f)]
        if mp4_files and format_options and '+' in format_options.get('quality', ''):
            # Use ffprobe to check if the mp4 file has audio
            try:
                import subprocess
                for mp4_file in mp4_files:
                    logger.info(f"Checking if {mp4_file} has audio streams")
                    cmd = [
                        'ffprobe', 
                        '-v', 'error', 
                        '-select_streams', 'a:0', 
                        '-show_entries', 'stream=codec_type', 
                        '-of', 'csv=p=0', 
                        mp4_file
                    ]
                    
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if result.stdout.strip() != 'audio':
                        logger.warning(f"MP4 file {mp4_file} has no audio stream! Will try to fix.")
                        
                        # If we have other audio sources, try to merge the audio into the MP4
                        audio_sources = [f for f in downloaded_files 
                                        if f.endswith(('.m4a', '.aac', '.mp3', '.opus')) 
                                        and os.path.exists(f) 
                                        and f != mp4_file]
                        
                        if audio_sources:
                            fixed_mp4_path = mp4_file.replace('.mp4', '_with_audio.mp4')
                            logger.info(f"Attempting to merge audio from {audio_sources[0]} into {fixed_mp4_path}")
                            
                            # Create a merged version with audio
                            fix_cmd = [
                                'ffmpeg',
                                '-y',
                                '-i', mp4_file,      # Video file
                                '-i', audio_sources[0],  # Audio file
                                '-c:v', 'copy',      # Copy video without re-encoding
                                '-c:a', 'aac',       # Convert audio to AAC
                                '-map', '0:v:0',     # Use video from first input
                                '-map', '1:a:0',     # Use audio from second input
                                '-shortest',         # Match shorter duration
                                fixed_mp4_path
                            ]
                            
                            logger.info(f"Running command: {' '.join(fix_cmd)}")
                            fix_result = subprocess.run(fix_cmd, capture_output=True)
                            
                            if os.path.exists(fixed_mp4_path) and os.path.getsize(fixed_mp4_path) > 0:
                                logger.info(f"Successfully created MP4 with audio: {fixed_mp4_path}")
                                # Replace the old file reference with the fixed one in our list
                                downloaded_files.append(fixed_mp4_path)
                                # Mark the original MP4 for potential removal from selection
                                if mp4_file in downloaded_files:
                                    downloaded_files.remove(mp4_file)
                    else:
                        logger.info(f"Confirmed {mp4_file} has audio streams")
            except Exception as e:
                logger.error(f"Error checking MP4 audio streams: {str(e)}")
        
        # First, scan for exact format matches
        for filepath in downloaded_files:
            if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
                continue
                
            file_ext = os.path.splitext(filepath)[1].lower()
            
            # Match requested video format
            if file_ext == '.mp4' and not video_file:
                video_file = filepath
                logger.info(f"Found primary video file (mp4): {video_file}")
                
            # Match requested audio format exactly
            if extract_audio and requested_audio_format and file_ext.lower() == f'.{requested_audio_format.lower()}':
                audio_file = filepath
                logger.info(f"Found exact audio format match ({requested_audio_format}): {audio_file}")
        
        # If we didn't find the exact audio format, look for any audio file as fallback
        if extract_audio and not audio_file:
            for filepath in downloaded_files:
                if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
                    continue
                    
                file_ext = os.path.splitext(filepath)[1].lower()
                if file_ext in ['.mp3', '.m4a', '.wav', '.aac', '.opus', '.flac']:
                    logger.info(f"Found fallback audio file: {filepath}")
                    # Only use it if we haven't found a better match
                    if not audio_file:
                        audio_file = filepath
                        logger.warning(f"Using fallback audio format {file_ext} instead of requested {requested_audio_format}")
        
        # If still no video file, accept any video format
        if not video_file:
            for filepath in downloaded_files:
                if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
                    continue
                    
                file_ext = os.path.splitext(filepath)[1].lower()
                if file_ext in ['.mp4', '.webm', '.mkv', '.mov', '.avi', '.flv']:
                    video_file = filepath
                    logger.info(f"Found fallback video file: {video_file}")
                    break
                        
        # GUARANTEED AUDIO FORMAT CONVERSION
        # If we're extracting audio, we MUST have the correct format
        requested_audio_format = audio_options.get('format', 'mp3')
        if extract_audio:
            logger.info(f"Ensuring audio is in {requested_audio_format} format")
            
            # First check if we already have the correct audio format
            found_correct_format = False
            for filepath in downloaded_files:
                if os.path.exists(filepath) and filepath.lower().endswith('.' + requested_audio_format.lower()):
                    audio_file = filepath
                    found_correct_format = True
                    logger.info(f"Found correctly formatted audio file: {audio_file}")
                    break
            
            if not found_correct_format:
                # Sort audio files by size (larger first) to get the best quality one
                audio_files = [f for f in downloaded_files if os.path.exists(f) and 
                              any(f.lower().endswith(ext) for ext in ['.m4a', '.aac', '.mp3', '.wav', '.opus', '.flac'])]
                
                # Sort by file size (descending)
                audio_files.sort(key=lambda f: os.path.getsize(f) if os.path.exists(f) else 0, reverse=True)
                
                if audio_files:
                    source_file = audio_files[0]
                    logger.info(f"Selected {source_file} for conversion to {requested_audio_format}")
                    
                    try:
                        # Create a descriptive filename
                        base_name = os.path.basename(source_file)
                        id_part = base_name.split('.')[0]  # Extract the ID part
                        target_file = os.path.join(os.path.dirname(source_file), 
                                                f"{id_part}.{requested_audio_format}")
                        
                        # Force overwrite if exists
                        if os.path.exists(target_file):
                            os.remove(target_file)
                        
                        # Use proper codec based on target format
                        codec_param = {
                            'mp3': ['-codec:a', 'libmp3lame', '-q:a', audio_options.get('quality', '192').replace('k', '')],
                            'aac': ['-c:a', 'aac', '-b:a', audio_options.get('quality', '192') + 'k'],
                            'm4a': ['-c:a', 'aac', '-b:a', audio_options.get('quality', '192') + 'k'],
                            'wav': ['-c:a', 'pcm_s16le'],
                            'flac': ['-c:a', 'flac']
                        }.get(requested_audio_format, ['-codec:a', 'libmp3lame'])
                        
                        # Build the command - make it very explicit
                        cmd = [
                            'ffmpeg', 
                            '-y',                  # Force overwrite
                            '-i', source_file,     # Input file
                            '-vn',                 # No video 
                            '-map', '0:a:0',       # Take first audio stream
                        ] + codec_param + [
                            '-f', requested_audio_format,  # Force output format
                            target_file            # Output file
                        ]
                        
                        logger.info(f"Running FFmpeg command: {' '.join(cmd)}")
                        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        
                        if os.path.exists(target_file) and os.path.getsize(target_file) > 0:
                            logger.info(f"Conversion successful: {target_file} ({os.path.getsize(target_file)} bytes)")
                            audio_file = target_file
                            downloaded_files.append(target_file)  # Add to downloaded files list
                        else:
                            logger.error(f"Conversion failed: output file missing or empty")
                            
                            # Fallback to direct ffmpeg command if the first attempt failed
                            logger.info("Trying alternative FFmpeg command")
                            alt_cmd = [
                                'ffmpeg',
                                '-y',                      # Force overwrite
                                '-i', source_file,         # Input file
                                '-vn',                     # No video
                                '-ar', '44100',            # Sample rate
                                '-ac', '2',                # Stereo
                                '-b:a', f"{audio_options.get('quality', '192')}k",  # Bitrate
                                target_file                # Output file
                            ]
                            
                            logger.info(f"Running alternative FFmpeg command: {' '.join(alt_cmd)}")
                            subprocess.run(alt_cmd, check=True)
                            
                            if os.path.exists(target_file) and os.path.getsize(target_file) > 0:
                                logger.info(f"Alternative conversion successful: {target_file}")
                                audio_file = target_file
                                downloaded_files.append(target_file)  # Add to downloaded files list
                    
                    except Exception as conv_error:
                        logger.error(f"Audio conversion failed: {str(conv_error)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.error("No audio files found for conversion")
            
        # If we didn't find specific file types, just take the first non-empty file
        if not video_file and not audio_file and downloaded_files:
            for filepath in downloaded_files:
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    if extract_audio:
                        audio_file = filepath
                    else:
                        video_file = filepath
                    break
        
        # Check that we have at least one file
        if not video_file and not audio_file:
            raise FileNotFoundError("Could not find any valid downloaded files")
        
        # STEP 10: Upload files
        cloud_urls = {}
        
        try:
            # Upload video file if available
            if video_file:
                logger.info(f"Uploading video file: {video_file} ({os.path.getsize(video_file)} bytes)")
                try:
                    video_cloud_url = upload_file(video_file)
                    cloud_urls['video'] = video_cloud_url
                    logger.info(f"Video upload successful: {video_cloud_url}")
                except Exception as e:
                    logger.error(f"Video upload failed: {str(e)}")
                    # This is critical for video-only downloads
                    if not extract_audio:
                        raise
            
            # Upload audio file if available
            if audio_file:
                logger.info(f"Uploading audio file: {audio_file} ({os.path.getsize(audio_file)} bytes)")
                try:
                    audio_cloud_url = upload_file(audio_file)
                    cloud_urls['audio'] = audio_cloud_url
                    logger.info(f"Audio upload successful: {audio_cloud_url}")
                except Exception as e:
                    logger.error(f"Audio upload failed: {str(e)}")
                    # This is critical for audio-only downloads
                    if extract_audio and not video_file:
                        raise
        
        except Exception as upload_error:
            logger.error(f"Upload process failed: {str(upload_error)}")
            raise RuntimeError(f"Failed to upload media: {str(upload_error)}")
        
        # STEP 11: Build response
        if not info:
            # Create minimal info if we don't have it
            info = {
                'id': 'unknown',
                'title': os.path.basename(media_url),
                'ext': os.path.splitext(video_file or audio_file)[1].lstrip('.') if (video_file or audio_file) else 'mp4',
                'format_id': 'unknown'
            }
        
        response = {"media": {}}
        
        # Add video info if present
        if 'video' in cloud_urls:
            response["media"] = {
                "media_url": cloud_urls['video'],
                "title": info.get('title', 'Unknown Title'),
                "format_id": info.get('format_id', 'unknown'),
                "ext": info.get('ext', 'mp4'),
                "resolution": info.get('resolution', 'unknown'),
                "width": info.get('width', 0),
                "height": info.get('height', 0),
                "fps": info.get('fps', 0),
                "video_codec": info.get('vcodec', 'unknown'),
                "audio_codec": info.get('acodec', 'unknown'),
                "download_timestamp": int(time.time())
            }
        
        # Add audio info if present
        if 'audio' in cloud_urls:
            # Determine actual format from file extension
            actual_format = os.path.splitext(audio_file)[1].lower().lstrip('.') if audio_file else audio_options.get('format', 'mp3')
            
            # If we requested mp3 but got something else, log a warning
            if actual_format != audio_options.get('format', 'mp3'):
                logger.warning(f"Requested audio format was {audio_options.get('format', 'mp3')} but actual format is {actual_format}")
            
            response["audio"] = {
                "audio_url": cloud_urls['audio'],
                "format": actual_format,  # Use actual format, not requested
                "quality": audio_options.get('quality', '192'),
                "requested_format": audio_options.get('format', 'mp3')  # Include requested format for debugging
            }
            
            # If there's only audio (no video), put basic info in media section too
            if 'video' not in cloud_urls:
                response["media"] = {
                    "media_url": cloud_urls['audio'],
                    "title": info.get('title', 'Unknown Title'),
                    "format_id": "audio_only",
                    "ext": audio_options.get('format', 'mp3'),
                    "download_timestamp": int(time.time())
                }
        
        return response, "/v1/media/download", 200
    
    except Exception as e:
        logger.error(f"Error processing download: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Make the error message more helpful
        error_message = f"Media download failed: {str(e)}"
        
        # Show details about the current state
        try:
            error_details = {
                "exception_type": type(e).__name__,
                "temp_directory": temp_dir,
                "media_url": media_url
            }
            
            if 'downloaded_files' in locals() and downloaded_files:
                error_details["downloaded_files"] = downloaded_files
                
            logger.error(f"Error details: {json.dumps(error_details)}")
        except:
            pass
        
        return error_message, "/v1/media/download", 500
    
    finally:
        # Always clean up the temporary directory
        try:
            if os.path.exists(temp_dir):
                logger.info(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as cleanup_error:
            logger.error(f"Failed to clean up temporary directory: {str(cleanup_error)}")
