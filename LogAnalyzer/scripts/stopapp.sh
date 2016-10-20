#!/bin/sh
ps -ef | grep LogAnalyzer | grep -v grep | awk -F " " '{print $2 }' | xargs kill -9
echo "Log Analyzer Stopped!"
