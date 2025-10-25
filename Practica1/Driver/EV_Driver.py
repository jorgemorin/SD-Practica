from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import sys

# KAFKA_BROKER = ['localhost:9092']
KAFKA_TOPIC = 'EVCharging'

#PUEDE QUE NO HAYA QUE USARLO
def send_user_data(kafka_broker: list, driver_id: str):
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            api_version=(4, 1, 0),
            request_timeout_ms=1000
        )
        print("[KAFKA PRODUCER] Successfully connected to broker.")

        message = f"DRIVER:{driver_id}"
        
        future = producer.send(KAFKA_TOPIC, value=message.encode('utf-8'))
        future.get(timeout=100)
        
        print(f"[KAFKA] Sent: {message}")
        time.sleep(1)

    except NoBrokersAvailable:
        print("[KAFKA PRODUCER] ERROR: No Kafka brokers available. Check address and ensure Kafka is running.")
    except Exception as e:
        print(f"[KAFKA PRODUCER] ERROR: An unexpected error occurred: {e}")
        
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            print("[KAFKA PRODUCER] Producer closed.")

def request_service(DRIVER_ID, CP_ID, kafka_broker):
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            api_version=(4, 1, 0),
            request_timeout_ms=1000
        )
        print("[KAFKA PRODUCER] Successfully connected to broker.")
        message = f"REQUEST:{DRIVER_ID}:{CP_ID}"
        
        future = producer.send(KAFKA_TOPIC, value=message.encode('utf-8'))
        future.get(timeout=100)
        
        print(f"[KAFKA] Sent: {message}")
        time.sleep(1)
    except NoBrokersAvailable:
        print("[KAFKA PRODUCER] ERROR: No Kafka brokers available. Check address and ensure Kafka is running.")
    except Exception as e:
        print(f"[KAFKA PRODUCER] ERROR: An unexpected error occurred: {e}")
        
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            print("[KAFKA PRODUCER] Producer closed.")
    

def main():
    if len(sys.argv) != 4:
        print("Usage: python EV_Driver.py# <BROKER_IP> <BROKER_PORT> <DRIVER_ID>")
        return
        
    KAFKA_BROKER = [f'{sys.argv[1]}:{sys.argv[2]}']
    DRIVER_ID = sys.argv[3]

    #send_user_data(KAFKA_BROKER, DRIVER_ID)
    #test_producer(KAFKA_BROKER)

    print("================================================")
    print(f"DRIVER {DRIVER_ID}")
    print("================================================")
    print("Selecciona una opcion:")
    print(" [1] Solicitar suministro de CP")
    print(" [2] Leer fichero de servicios")
    opcion = input("> ")

    if opcion == '1':
        try:
            CP_ID = int(input("Seleccione un Charging Point para solicitar suministro: "))
        except ValueError:
            print("ID de Charging Point invalido.")
            return

        request_service(DRIVER_ID, CP_ID, KAFKA_BROKER)

if __name__ == "__main__":
    main()