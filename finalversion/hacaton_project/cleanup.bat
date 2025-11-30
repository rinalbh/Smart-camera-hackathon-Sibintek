@echo off
echo Очистка временных файлов...
timeout /t 3 /nobreak >nul
rmdir /s /q "local_s3_storage" 2>nul
rmdir /s /q "temp_videos" 2>nul  
rmdir /s /q "temp" 2>nul
echo Готово!
pause
