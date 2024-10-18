from google.auth import default

credentials, project = default()
print(f"Credentials: {credentials}")
print(f"Project: {project}")
