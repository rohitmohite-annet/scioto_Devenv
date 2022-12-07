from socketlabs.injectionapi import SocketLabsClient
from socketlabs.injectionapi.message.__imports__ import Attachment,BasicMessage,EmailAddress
serverId = 36101
injectionApiKey = "Qz89ZcBp24EfPg6x7L5J"
message = BasicMessage()
message.from_email_address = EmailAddress("rohit.mohite@annet.com")
message.add_to_email_address("rohit.mohite@annet.com")


def success_ran():
    message.subject = 'File ran successfully'
    message.html_body=f'''<!DOCTYPE html>
    <html>
    <body>

    <p><span style='font-size:15px;line-height:115%;font-family:"Calibri","sans-serif";'>Cron job ran successfully for NOI Per sq.ft.</span></p> 

    <div style="margin:auto;text-align: center;">
    <img src="https://www.4seeanalytics.com/dev/public/vendor/images/4see-portal-final.png" alt="logo">
    </div>

    </body>
    </html>
    '''
    # send the message
    client = SocketLabsClient(serverId, injectionApiKey)
    response = client.send(message)
    return response

def sql_conn_fail():
    message.subject = 'SQL connection failure'
    message.html_body = f'''<!DOCTYPE html>
            <html>
            <body>

            <p><span style='font-size:15px;line-height:115%;font-family:"Calibri","sans-serif";'>SQL server connection failed.</span></p>
            <p><span style='font-size:15px;line-height:115%;font-family:"Calibri","sans-serif";'>&nbsp;Please add server IP to firewall</span></p>
            <p><br></p>

            <div style="margin:auto;text-align: center;">
            <img src="https://www.4seeanalytics.com/dev/public/vendor/images/4see-portal-final.png" alt="logo">
            </div>

            </body>
            </html>
            '''
    # send the message
    client = SocketLabsClient(serverId, injectionApiKey)
    response = client.send(message)
    print(response)
    return response


def cron_fail():
    message.subject = 'Crone Job failure'
    message.html_body=f'''<!DOCTYPE html>
    <html>
    <body>

    <p><span style='font-size:15px;line-height:115%;font-family:"Calibri","sans-serif";'>Cron job failed.</span></p>

    <div style="margin:auto;text-align: center;">
    <img src="https://www.4seeanalytics.com/dev/public/vendor/images/4see-portal-final.png" alt="logo">
    </div>

    </body>
    </html>
    '''
    # send the message
    client = SocketLabsClient(serverId, injectionApiKey)
    response = client.send(message)
    return response




