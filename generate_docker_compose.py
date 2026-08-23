import sys

def generate_docker_compose(num_clients):
    compose_content = """services:
  server:
    build:
      context: ./services/server
      dockerfile: Dockerfile
    container_name: server
    environment:
      - PYTHONUNBUFFERED=1
      - SERVER_HOST=server
      - SERVER_PORT=5678
    """

    for i in range(num_clients):
        compose_content += f"""
  client_{i}:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_{i}
    depends_on:
      - server
    environment:
      - AGENCY_ID={i}
      - SERVER_HOST=server
      - SERVER_PORT=5678
    """

    with open("docker-compose.yml", "w") as f: 
        f.write(compose_content)

    print(f"'docker-compose.yaml' generado con éxito con {num_clients} clientes.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 generate_docker_compose.py <cantidad_de_clientes>")
        sys.exit(1)
    
    try:
        num_clients = int(sys.argv[1])
        if num_clients < 1:
            raise ValueError
        generate_docker_compose(num_clients)
    except ValueError:
        print("Error: Por favor, ingresa un número entero mayor a 0.")
        sys.exit(1)