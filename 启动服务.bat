@echo off
CHCP 65001
title Z.ai OpenAI 代理服务
echo 正在启动代理服务 (端口 30016)...
echo.

:: 切换到脚本所在目录
cd /d "%~dp0"

:: 启动服务
python server.py

echo.
if %errorlevel% neq 0 (
    echo [错误] 程序异常退出，请检查上方报错信息。
) else (
    echo 服务已正常停止。
)
pause
