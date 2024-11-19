import subprocess
from pathlib import Path


def stream_logs(container):
    for line in container.logs(stream=True, follow=True):
        print(f"Container log: {line.decode('utf-8').strip()}")


def generate_ssl_certs(certs_dir: Path):
    certs_dir.mkdir(parents=True, exist_ok=True)

    # File paths
    ca_key = certs_dir / "ca-key.pem"
    ca_cert = certs_dir / "ca.pem"
    server_key = certs_dir / "server-key.pem"
    server_cert = certs_dir / "server-cert.pem"
    client_key = certs_dir / "client-key.pem"
    client_cert = certs_dir / "client-cert.pem"
    server_csr = certs_dir / "server.csr"
    client_csr = certs_dir / "client.csr"

    # Create CA key and certificate
    subprocess.run(["openssl", "genrsa", "-out", str(ca_key), "2048"], check=True)

    subprocess.run(
        [
            "openssl",
            "req",
            "-new",
            "-x509",
            "-key",
            str(ca_key),
            "-out",
            str(ca_cert),
            "-days",
            "3650",
            "-subj",
            "/CN=MySQL Test CA",
        ],
        check=True,
    )

    # Create server key and CSR (Certificate Signing Request)
    subprocess.run(["openssl", "genrsa", "-out", str(server_key), "2048"], check=True)

    subprocess.run(
        ["openssl", "req", "-new", "-key", str(server_key), "-out", str(server_csr), "-subj", "/CN=MySQL Server"],
        check=True,
    )

    # Sign the server CSR with the CA to create the server certificate
    subprocess.run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(server_csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(server_cert),
            "-days",
            "3650",
        ],
        check=True,
    )

    # Create client key and CSR
    subprocess.run(["openssl", "genrsa", "-out", str(client_key), "2048"], check=True)

    subprocess.run(
        ["openssl", "req", "-new", "-key", str(client_key), "-out", str(client_csr), "-subj", "/CN=MySQL Client"],
        check=True,
    )

    # Sign the client CSR with the CA to create the client certificate
    subprocess.run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(client_csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(client_cert),
            "-days",
            "3650",
        ],
        check=True,
    )

    # Clean up CSR files
    server_csr.unlink()
    client_csr.unlink()
