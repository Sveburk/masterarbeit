# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Compilation Commands
- LaTeX: `latexmk -pdf -shell-escape main.tex` (or `pdflatex -shell-escape main.tex`)
- Python scripts: No formal build process, run directly with Python 3

## Python Dependencies
Install with: `pip install pandas openai pillow base64 xml json os csv time re xattr plistlib io fitz`

## Coding Guidelines
- **Python Style**: Use snake_case for variables and functions
- **Comments**: Include German comments for clarity
- **Error Handling**: Use try/except blocks with specific exceptions
- **File Paths**: Use raw strings (r"path/to/file") for file paths
- **Documentation**: Document purpose of scripts at the top with docstrings
- **Variable Names**: Use descriptive German/English variable names
- **Constants**: Define constants at the top of scripts
- **Formatting**: Maintain consistent indentation (4 spaces) and line breaks

## Project Structure
- LaTeX files in `1_MA_Arbeit/`
- Literature in `2_MA_Literature/`
- Project data and scripts in `3_MA_Project/`
- Working group materials in `4_MA_Arbeitsgruppe/`