import subprocess
import os

# Running the Extraction Jobs:

org_dir = os.getcwd()

venv_python_path = r"C:\Users\shusant.sapkota\ETLProject\pythonProject\venv\Scripts\python"
os.chdir("src/extract/")
contents = os.listdir()
e_scripts = [f for f in contents if os.path.isfile(f)]

for script in e_scripts:
    subprocess.run([venv_python_path, script])


# Running the Transformation and Loading Jobs:
os.chdir(org_dir)
os.chdir("src/loadtransform/")
contents = os.listdir()
tl_scripts = [f for f in contents if os.path.isfile(f)]

for script in tl_scripts:
    subprocess.run([venv_python_path, script])

