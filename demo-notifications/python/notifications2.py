import boto3
import os
import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Cargar las variables de entorno desde el archivo .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

class NotificationManager:
    def __init__(self):
        self.dynamodb = boto3.client(
            'dynamodb',
            aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
            region_name='us-east-2'
        )
        self.sns_client = boto3.client(
            'sns',
            aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
            region_name='us-east-2'
        )
        self.table_name = 'notifications'
        
        
    def create_notifications_table(self):
        try:
            response = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'UserID_TypeBehavior_BeautySalonID',
                        'KeyType': 'HASH'  # Clave de partición
                    },
                    {
                        'AttributeName': 'Timestamp',
                        'KeyType': 'RANGE'  # Clave de ordenación
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'UserID_TypeBehavior_BeautySalonID',
                        'AttributeType': 'S'  # Tipo de atributo: String
                    },
                    {
                        'AttributeName': 'Timestamp',
                        'AttributeType': 'S'  # Tipo de atributo: String
                    },
                    {
                        'AttributeName': 'TypeBehavior',
                        'AttributeType': 'S'  # Tipo de atributo: String
                    },
                    {
                        'AttributeName': 'BeautySalonID',
                        'AttributeType': 'S'  # Tipo de atributo: String
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'TypeBehavior-BeautySalonID-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'TypeBehavior',
                                'KeyType': 'HASH'  # Clave de partición
                            },
                            {
                                'AttributeName': 'BeautySalonID',
                                'KeyType': 'RANGE'  # Clave de ordenación
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            print("Table created successfully!")
            
            # Esperar hasta que la tabla esté en estado ACTIVE
            waiter = self.dynamodb.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name)
            print("Table is now active!")
            
            return response
        except self.dynamodb.exceptions.ResourceInUseException:
            print("Table already exists.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def update_notifications(self, user_id, email, type_to_behavior, beauty_salon_id=None, date=None, time=None, service=None, offer_id=None, description=None, reminder_id=None):
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        user_id_type_behavior_beauty_salon_id = f"{user_id}#{type_to_behavior}#{beauty_salon_id}"

        item = {
            'UserID_TypeBehavior_BeautySalonID': {'S': user_id_type_behavior_beauty_salon_id},
            'Timestamp': {'S': timestamp},
            'Email': {'S': email},
            'TypeBehavior': {'S': type_to_behavior},  
            'Active': {'BOOL': True}
        }

        if beauty_salon_id is not None:
            item['BeautySalonID'] = {'S': beauty_salon_id}  

        if type_to_behavior == 'Subscription':
            # Lógica específica para 'Subscription'
            pass
        elif type_to_behavior == 'Reminder':
            # Lógica específica para 'Reminder'
            if date is not None:
                item['Date'] = {'S': date}
            if time is not None:
                item['Time'] = {'S': time}
            if service is not None:
                item['Service'] = {'S': service}
            if reminder_id is not None:
                item['ReminderID'] = {'S': reminder_id}
        elif type_to_behavior == 'Offer':
            # Lógica específica para 'Offer'
            if offer_id is not None:
                item['OfferID'] = {'S': offer_id}
            if description is not None:
                item['Description'] = {'S': description}

        try:
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=item
            )
            print("Notification updated successfully.")
        except Exception as e:
            print(f"Error updating notification: {e}")

    def subscribe_to_sns_topic(self, email):
        try:
            topic_arn = os.getenv('ARN')
            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol='email',
                Endpoint=email
            )
            return response
        except ClientError as e:
            return {"status": "error", "message": str(e)}

    def send_offer_notification(self, email, beauty_salon_id, offer_id, description):
        try:
            subject = "New Offer Available"
            body = f"Hello,\n\nBeauty salon {beauty_salon_id} has a new offer: {description}."
            response = self.sns_client.publish(
                TopicArn=os.getenv('ARN'),
                Message=body,
                Subject=subject,
                MessageAttributes={
                    'email': {
                        'DataType': 'String',
                        'StringValue': email
                    }
                }
            )
            return response
        except ClientError as e:
            return {"status": "error", "message": str(e)}

    def send_reminder_notification(self, email, user_id, beauty_salon_id, date, time, service):
        try:
            subject = "Appointment Reminder"
            body = f"Hello {user_id},\n\nThis is a reminder for your appointment at beauty salon {beauty_salon_id} on {date} at {time} for {service}."
            response = self.sns_client.publish(
                TopicArn=os.getenv('ARN'),
                Message=body,
                Subject=subject,
                MessageAttributes={
                    'email': {
                        'DataType': 'String',
                        'StringValue': email
                    }
                }
            )
            return response
        except ClientError as e:
            return {"status": "error", "message": str(e)}

    def send_unsubscription_notification(self, email, user_id, beauty_salon_id):
        try:
            subject = "Unsubscription Confirmation"
            body = f"Hello {user_id},\n\nYou have successfully unsubscribed from beauty salon {beauty_salon_id}."
            response = self.sns_client.publish(
                TopicArn=os.getenv('ARN'),
                Message=body,
                Subject=subject,
                MessageAttributes={
                    'email': {
                        'DataType': 'String',
                        'StringValue': email
                    }
                }
            )
            return response
        except ClientError as e:
            return {"status": "error", "message": str(e)}

    def send_offer_notification_to_all_followers(self, beauty_salon_id, offer_id, description):
        try:
            # Consultar la tabla para obtener todos los seguidores del salón de belleza
            response = self.dynamodb.query(
                TableName=self.table_name,
                IndexName='TypeBehavior-BeautySalonID-index',
                KeyConditionExpression='TypeBehavior = :type_behavior AND BeautySalonID = :beauty_salon_id',
                ExpressionAttributeValues={
                    ':type_behavior': {'S': 'Subscription'},
                    ':beauty_salon_id': {'S': beauty_salon_id}
                }
            )
            
            # Iterar sobre los ítems y enviar una notificación a cada seguidor
            for item in response.get('Items', []):
                email = item['Email']['S']
                self.send_offer_notification(email, beauty_salon_id, offer_id, description)
            
            return {"status": "success", "message": "Notifications sent to all followers"}
        except ClientError as e:
            return {"status": "error", "message": str(e)}

    def get_recent_notifications_by_type_and_salon(self, type_behavior, beauty_salon_id):
        try:
            response = self.dynamodb.query(
                IndexName='TypeBehavior-BeautySalonID-index',  
                TableName=self.table_name,  # Reemplaza 'YourTableName' con el nombre de tu tabla
                KeyConditionExpression='TypeBehavior = :type_behavior AND BeautySalonID = :beauty_salon_id',
                ExpressionAttributeValues={
                    ':type_behavior': {'S': type_behavior},
                    ':beauty_salon_id': {'S': beauty_salon_id}
                }
            )
            return response.get('Items', [])
        except self.dynamodb_client.exceptions.ValidationException as e:
            print(f"Error de validación al obtener las notificaciones recientes: {e}")
        except Exception as e:
            print(f"Error al obtener las notificaciones recientes: {e}")
        return []
# Ejemplo de uso
if __name__ == "__main__":
    manager = NotificationManager()
    user_id = 'NicoGod2'
    email = 'nxhm2013@gmail.com'
    beauty_salon_id = 'Salon123'
    offer_id = 'Offer123'
    description = '50% off on all services'
    date = '2023-10-01'
    time = '10:00 AM'
    service = 'Haircut'
    reminder_id = 'Reminder123'

    # Crear la tabla de notificaciones
    #manager.create_notifications_table()

    # Actualizar notificaciones
    update_response = manager.update_notifications(user_id, email, 'Subscription', beauty_salon_id)

    # Actualizar notificaciones para recordatorios
    update_response = manager.update_notifications(user_id, email, 'Reminder', beauty_salon_id, date, time, service, reminder_id=reminder_id)

    # Actualizar notificaciones para ofertas
    update_response = manager.update_notifications(user_id, email, 'Offer', beauty_salon_id, offer_id=offer_id, description=description)

    # Subscribe to SNS topic
    #subscribe_response = manager.subscribe_to_sns_topic(email)
    #print(f"Subscribe response: {subscribe_response}")

    # Send offer notification
    offer_response = manager.send_offer_notification(email, beauty_salon_id, offer_id, description)
    print(f"Offer response: {offer_response}")

    # Send reminder notification
    reminder_response = manager.send_reminder_notification(email, user_id, beauty_salon_id, date, time, service)
    print(f"Reminder response: {reminder_response}")

    # Send unsubscription notification
    #unsubscription_response = manager.send_unsubscription_notification(email, user_id, beauty_salon_id)
    #print(f"Unsubscription response: {unsubscription_response}")

    # Send offer notification to all followers
    manager.send_offer_notification_to_all_followers(beauty_salon_id, offer_id, description)

    recent_offers = manager.get_recent_notifications_by_type_and_salon('Offer', beauty_salon_id)
    print(f"Recent offers: {recent_offers}")
    print("")
    # Obtener notificaciones recientes por tipo y salón
    recent_notifications = manager.get_recent_notifications_by_type_and_salon('Reminder', beauty_salon_id)
    print(f"Recent notifications: {recent_notifications}")