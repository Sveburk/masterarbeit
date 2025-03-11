# README: LaTeX Project on GitHub

## Overview
This repository contains a LaTeX project that uses multiple packages for document formatting, mathematical expressions, bibliographies, and code highlighting. Below, you will find instructions on how to set up and compile this project.

## Prerequisites
Before using this LaTeX project, ensure you have the following installed:

- **TeX Live** (Recommended for full package support) or **MiKTeX**
- **Biber** (For bibliography management)
- **Python** (For `minted` package, which requires Pygments)
- **Visual Studio Code** with the **Visual Studio Code LaTeX** extension (Recommended for LaTeX editing and compilation)

### Required LaTeX Packages
The following LaTeX packages are used in this project:

```latex
\usepackage[utf8]{inputenc} % UTF-8 encoding
\usepackage[T1]{fontenc} % Font encoding
\usepackage[fixed]{fontawesome5} % FontAwesome icons
\usepackage{amsmath,amssymb} % Math symbols
\usepackage{xcolor} % Colors
\usepackage{tcolorbox} % Colored boxes
\usepackage{afterpage} % Post-page commands
\usepackage{hyperref} % Hyperlinks
\usepackage{graphicx} % Image handling
\usepackage{subcaption} % Subfigures
\usepackage{setspace} % Line spacing
\usepackage{transparent} % Transparency settings
\usepackage{tikz} % Drawing and graphics
\usepackage{eso-pic} % Background images
\usepackage{fvextra} % Enhanced verbatim
\usepackage{csquotes} % Quotation formatting
\usepackage[authordate,backend=biber,language=ngerman]{biblatex-chicago} % Bibliography
\addbibresource{assets/Literature_Bib/literatur.bib}
\usepackage{minted} % Syntax highlighting for code
\usepackage[ngerman]{babel} % German language support
\usepackage{pifont} % Additional symbols
\usepackage{xcolor} % Colors for syntax highlighting
```

## Required Python Packages
The repository also contains Python scripts that require the following dependencies:

```sh
pip install pandas openai pillow base64 xml json os csv time re xattr plistlib io
```

## Installation Instructions
### 1. Clone the Repository
```sh
git clone https://github.com/your-username/your-latex-repo.git
cd your-latex-repo
```

### 2. Install Required Packages
Most of the packages should be available in **TeX Live** or **MiKTeX**. If you need to install missing packages, use:

For **TeX Live**:
```sh
tlmgr install <package-name>
```
For **MiKTeX**:
```sh
mpm --install=<package-name>
```

### 3. Install `minted` Dependencies
Since the `minted` package requires **Pygments**, install it via:
```sh
pip install pygments
```

### 4. Install VS Code LaTeX Extension
If you are using **VS Code**, install the **Visual Studio Code LaTeX** extension for improved LaTeX editing and compilation.

### 5. Compile the LaTeX Document
If you are using **TeX Live** with `pdflatex`, run:
```sh
pdflatex -shell-escape main.tex
biber main
pdflatex -shell-escape main.tex
pdflatex -shell-escape main.tex
```

Alternati
