import socket
import numpy as np
from sklearn.neural_network import MLPClassifier
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import threading
import joblib

def load_model():
    # Cargar el modelo MLP entrenado desde el archivo mlp_model.joblib
    return joblib.load('svm_classifier_model.joblib')

def process_stream(_, rdd, mlp_model, client_socket):
    # Convertir los datos a un arreglo numpy y hacer predicciones
    data_array = np.array(rdd.collect(), dtype=np.float64).reshape(-1, 784)
    if data_array.shape[0] > 0:
        predictions = mlp_model.predict(data_array)
        print("Predicciones:", predictions)

        # Enviar las predicciones al cliente
        response_bytes = predictions.tobytes()
        response_length = len(response_bytes).to_bytes(4, byteorder='big')
        client_socket.sendall(response_length)
        print("Mostrando los datos que envie", response_length)
        client_socket.sendall(response_bytes)
        print("Mostrando los datos que envie 2", response_bytes)

def handle_client(client_socket, mlp_model, sc):
    # Procesar la transmisión del cliente
    data_length_bytes = client_socket.recv(4)
    data_length = int.from_bytes(data_length_bytes, byteorder='big')
    data = client_socket.recv(data_length)
    data_array = np.frombuffer(data, dtype=np.float64).reshape(-1, 784)
    print("Mostrando la data que recibe del cliente", data_array)

    # Procesar la transmisión utilizando Spark Streaming
    ssc = StreamingContext(sc, 3)  # Intervalo de 3 segundos (ajusta según tus necesidades)

    # Crear un DStream que procesa los datos recibidos del cliente
    rdd_queue = []
    rdd_queue.append(sc.parallelize(data_array))
    inputStream = ssc.queueStream(rdd_queue)
    inputStream.foreachRDD(lambda rdd: process_stream(None, rdd, mlp_model, client_socket))

    # Iniciar el contexto de Spark Streaming
    ssc.start()

    # Esperar a que el contexto de Spark Streaming termine
    # Aquí utilizamos un temporizador para terminar el contexto después de 5 segundos
    threading.Timer(5, lambda: ssc.stop(stopSparkContext=True, stopGraceFully=True)).start()

def start_server():
    # Crear el SparkContext antes de iniciar el servidor
    conf = SparkConf().setAppName("StreamingApp")
    sc = SparkContext(conf=conf)

    # Cargar el modelo MLP entrenado desde el archivo mlp_model.joblib
    mlp_model = load_model()

    # Establecer la conexión con el cliente
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('192.168.0.108', 1908))
    server_socket.listen(5)
    print("Esperando conexiones de clientes...")

    while True:
        # Aceptar conexiones de clientes
        client_socket, client_address = server_socket.accept()
        print("Cliente conectado:", client_address)

        # Crear un hilo para manejar al cliente
        client_thread = threading.Thread(target=handle_client, args=(client_socket, mlp_model, sc))
        client_thread.start()

if __name__ == "__main__":
    start_server()
