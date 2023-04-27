import subprocess

def run_script(script_path):
    process = subprocess.Popen(['python3', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f'Started script {script_path} with pid {process.pid}')
    print(f'Stdout: {process.stdout.read().decode("utf-8")}')
    print(f'Stderr: {process.stderr.read().decode("utf-8")}')

def main():
    run_script('ml_producer/ml_telegram.py')
    run_script('ml_producer/ml_discord.py')

main()