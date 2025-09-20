## Markdown in VS Code

- (Ctrl+K V) Preview side-by-side

- Headings: #, ##, ###

- Format: **bold** *italics* `code`

- > blockquote

## Project Setup

Note: I'm using Windows here. Since `requirements.text` was exported from a macOS/conda-forge environment, the command `conda create --file` tries to install those exact builds, which don’t exist on win-64. The `environment-windows.yml` drops macOS build strings, keeps versions, and uses conda-forge to create the environment on Windows without errors.

1. Open Git Bash

2. `cd Desktop/wid-datathon`

3. Naming my new environment `wid_env` with command: 

    `conda env create -f environment-windows.yml -n wid_env`

4. `conda activate wid_env`

5. Register conda environment as a Jupyter kernel.

    `python -m ipykernel install --user --name=wid_env --display-name "Python (wid_env)`

6. `code .` to launch VS Code. Install Python and Jupyter Notebook extensions if you haven't already.

7. (Ctrl+Shift+P) Open command palette. Find "Python: Select Interpreter" and choose `wid_env`.

8. In `/notebooks`, create a new folder for your individual exploratory data analysis. Create/move your `.ipynb` file into the folder. Verify that the kernel in the top right is `wid_env`.

10. Terminal inside VS Code: `conda activate wid_env`.