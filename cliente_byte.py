import socket
import time
import numpy as np

def generate_random_data():
    data = np.random.rand(5, 784)
    return data
def receive_data(client_socket):
    # Recibir la longitud de los datos como un entero de 4 bytes
    data_length_bytes = client_socket.recv(4)
    data_length = int.from_bytes(data_length_bytes, byteorder='big')

    # Recibir los datos en bytes
    data = b''
    while len(data) < data_length:
        data += client_socket.recv(data_length - len(data))

    # Convertir los datos en bytes a un arreglo numpy
    response = np.frombuffer(data, dtype=np.float64).reshape(5, 1)
    return response
def send_data(data):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('192.168.0.108', 1908)

    try:
        client_socket.connect(server_address)

        # Convertir los datos a bytes
        data_bytes = data.tobytes()

        # Enviar la longitud de los datos primero (como 4 bytes)
        data_length = len(data_bytes).to_bytes(4, byteorder='big')
        client_socket.sendall(data_length)

        # Enviar los datos en formato binario
        client_socket.sendall(data_bytes)

        print("Datos enviados con éxito.")


        # Esperar la respuesta del servidor
        #response_length_bytes = client_socket.recv(4)
        #response_length = int.from_bytes(response_length_bytes, byteorder='big')
        #response_bytes = client_socket.recv(response_length)

        # Esperar la respuesta del servidor
        response = receive_data(client_socket)
        print(response)

    except socket.error as e:
        print("Error de socket al enviar/recibir los datos:", e)
    except Exception as e:
        print("Error al enviar/recibir los datos:", e)
    finally:
        client_socket.close()

if __name__ == "__main__":
    while True:
        data_to_send = generate_random_data()
        send_data(data_to_send)
        time.sleep(5)
