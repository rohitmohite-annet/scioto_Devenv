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


def htmlfile(insight_title,insight_message,insight_graph,insight_details_link):
    html_content = f'''<html>
<head>
<title>mailer_4seeInsights2</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
</head>
<body bgcolor="#FFFFFF" leftmargin="0" topmargin="0" marginwidth="0" marginheight="0">
<!-- Save for Web Slices (mailer_4seeInsights2.psd) -->
<table id="Table_01" width="680" height="1621" border="0" cellpadding="0" cellspacing="0" align="center" style="font-family:Arial Narrow, Helvetica, sans-serif; border:1px solid #999;">
	<tr>
		<td colspan="12" height="53"></td>
	</tr>
	<tr>
		<td colspan="12">
			<a href="#" target="_blank" style="text-align:center;width: 100%;float: left;"><img src="https://qascioto4seeapp.azurewebsites.net/public/vendor/images/logo/mailer_4seeInsights2_03.jpg" width="172" height="90" alt="" style="text-align: center;"></a></td>
	</tr>
	<tr>
		<td colspan="12">
			<p style="margin-top:35px; margin-bottom:35px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:34px; font-weight:bold; color:#1A7CAB;">INSIGHTS</p>
        </td>
	</tr>
	<tr>
		<td width="76"></td>
		<td colspan="10" bgcolor="#f6f6f6" style="border-radius:40px;">
			<div class="alignment" align="center" style="line-height:10px; margin-top:35px;"><img src="https://qascioto4seeapp.azurewebsites.net/public/vendor/images/logo/icon_dollor_up.png" style="display: block; height: 111px; border: 0; width: 60px; max-width: 100%;" width="60px" alt="Icon dollor up" title="Icon dollor up"></div>
            <div style="font-family:Arial Narrow, Helvetica, sans-serif;">
				<p style="font-size:26px; color:#1A7CAB; font-weight:bold; margin-top:20px; padding-top:15px; padding-bottom:5px; margin-bottom:10px; text-align: center;"><span><strong>{insight_title}</strong></span></p>
			</div>
            <div style="font-family: Arial Narrow, Helvetica, sans-serif;">
        		<p style="margin: 0; text-align: center; font-size: 20px; color:#282828; padding-left:20px; padding-right:20px; padding-top:5px;"><strong>{insight_message}</strong></p>
			</div>
            <div style="font-family: Arial Narrow, Helvetica, sans-serif; text-align:center; margin-top:15px;"><img align="center" alt="graph" height="347" src="data:image/png;base64,{insight_graph}" width="484"/></div>
            <div class="alignment" align="center"><a href="{insight_details_link}" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top:5px;padding-bottom:5px; margin-bottom:35px; font-family:Arial Narrow, Helvetica, sans-serif; font-weight:600; text-align:center; margin-top:16px;"><span style="padding-left:25px;padding-right:25px;font-size:15px;display:inline-block;"><span dir="ltr" style="word-break: break-word;"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">DETAILS</span></span></span></a>
		</div>
        </td>
		<td width="76"></td>
	</tr>
	<tr>
		<td colspan="12">
			<p style="margin-top:25px; margin-bottom:35px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:24px; font-weight:bold; color:#1A7CAB;">WERE THESE INSIGHTS USEFUL ?</p>
        </td>
	</tr>
	<tr>
		<td colspan="2" height="81"></td>
		<td colspan="4"> 
			<div class="alignment" align="center" style="
    float: left;
"><a href="#" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top: 10px;padding-bottom: 10px;margin-bottom:35px;font-family:Arial Narrow, Helvetica, sans-serif;text-align:center;margin-top:0;"><span style="padding-left: 45px;padding-right: 45px;font-size: 20px;display:inline-block;font-weight: bold;"><span dir="ltr" style="word-break: break-word;"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">YES</span></span></span></a>
		</div>
        </td>
		
		<td colspan="4" style="">
			<div class="alignment" align="center" style="
    float: left;
"><a href="#" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top: 10px;padding-bottom: 10px;margin-bottom:35px;font-family:Arial Narrow, Helvetica, sans-serif;text-align:center;margin-top:0;"><span style="padding-left: 45px;padding-right: 45px;font-size: 20px;display:inline-block;font-weight: bold;"><span dir="ltr" style="word-break: break-word;"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">NO</span></span></span></a>
		</div>	
        </td>
		<td colspan="2" height="81"></td>
	</tr>
	<tr>
		<td colspan="12" style="padding-bottom: 30px; padding-left: 40px; padding-right: 40px;">
			<hr></hr></td>
	</tr>
	<tr>
		
		<td colspan="12">
			<a href="#" target="_blank" style="text-align: center;width: 100%;float: left;"><img src="https://qascioto4seeapp.azurewebsites.net/public/vendor/images/logo/mailer_4seeInsights2_17.jpg" width="267" height="69" alt="" style="
    text-align: center;
"></a></td>

	</tr>
	<tr>
		<td colspan="12">
			<p style="margin-left:45px; margin-right:45px; margin-bottom:10px; margin-top:35px; text-align:left; font-family:Arial, Helvetica, sans-serif; font-size:11px; color:#758c9d;">© 2022 The Annet Group Inc. All rights reserved. 4see Analytics, Retransform and the associated logos are registered trademarks of The Annet Group Inc.. All other trademarks are the property of their respective owners. Log in to  manage all your notification preferences in <a href="" target="_blank" style="text-decoration:none;">Email Settings</a>. This email was sent to <a href="" target="_blank" style="text-decoration:none;">username email</a> and is intended for that recipient. You've been sent this as you've chosen to receive our updates.</p>
      <p style="margin-left:45px; margin-right:45px; margin-bottom:10px; text-align:left; font-family:Arial, Helvetica, sans-serif; font-size:11px; color:#758c9d;">Retransform Lts, 22 Bishopsgate London EC2N 4BQ. | The Annet Group Dallas 6330 Lyndon B. Johnson Fwy.Suite 234-A Dallas, TX 75240 | The Annet Group Asia D204, Dubai Silicon Oasis Headquarter Bldg, P.O. Box 341472, Dubai Silicon Oasis, Dubai, U.A.E | The Annet Group Oceania Level 1, 207-209, Marrickville Road, Marrickville NSW 2204, Australia | The Annet Group Central Asia Evergreen Industrial Estate, Shakti Mills Lane, Mahalaxmi, Mumbai 400011, India </p>	
        </td>
	</tr>
	<tr>
		<td colspan="12">
			<p style="margin-top:25px; margin-bottom:150px; margin-left:16px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:14px; font-weight:bold; color:#96ae47;"><a style="text-decoration:none; color:#96ae47;" href="" target="_blank">UNSUBSCRIBE</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">EMAIL PREFERENCES</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">PRIVACY POLICY</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">CONTACT US</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">HELP</a></p>	
        </td>
	</tr>
	<tr>
		<td>
			<img src="images/spacer.gif" width="76" height="1" alt=""></td>
		<td width="36">
			<img src="images/spacer.gif" width="36" height="1" alt=""></td>
		<td width="95">
			<img src="images/spacer.gif" width="95" height="1" alt=""></td>
		<td width="49">
			<img src="images/spacer.gif" width="48" height="1" alt=""></td>
		<td width="31">
			<img src="images/spacer.gif" width="31" height="1" alt=""></td>
		<td width="138">
			<img src="images/spacer.gif" width="138" height="1" alt=""></td>
		<td width="5">
			<img src="images/spacer.gif" width="3" height="1" alt=""></td>
		<td width="48">
			<img src="images/spacer.gif" width="47" height="1" alt=""></td>
		<td>
			<img src="images/spacer.gif" width="45" height="1" alt=""></td>
		<td width="79">
			<img src="images/spacer.gif" width="79" height="1" alt=""></td>
		<td width="6">
			<img src="images/spacer.gif" width="6" height="1" alt=""></td>
		<td>
			<img src="images/spacer.gif" width="76" height="1" alt=""></td>
	</tr>
</table>
<!-- End Save for Web Slices -->
</body>
</html>
'''
    return html_content
#
#     html_content = f'''<html>
# <head>
# <title>mailer_4seeInsights2</title>
# <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
# </head>
# <body bgcolor="#FFFFFF" leftmargin="0" topmargin="0" marginwidth="0" marginheight="0">
# <!-- Save for Web Slices (mailer_4seeInsights2.psd) -->
# <table id="Table_01" width="680" height="1621" border="0" cellpadding="0" cellspacing="0" align="center" style="font-family:Arial Narrow, Helvetica, sans-serif; border:1px solid #999;">
# 	<tr>
# 		<td colspan="12" width="680" height="53"></td>
# 	</tr>
# 	<tr>
# 		<td colspan="4">
# 			<img src="images/mailer_4seeInsights2_02.jpg" width="255" height="90" alt=""></td>
# 		<td colspan="3">
# 			<a href="#" target="_blank"><img src="images/mailer_4seeInsights2_03.jpg" width="172" height="90" alt=""></a></td>
# 		<td colspan="5">
# 			<img src="images/mailer_4seeInsights2_04.jpg" width="253" height="90" alt=""></td>
# 	</tr>
# 	<tr>
# 		<td colspan="12">
# 			<p style="margin-top:35px; margin-bottom:35px; margin-left:30px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:34px; font-weight:bold; color:#1A7CAB;">INSIGHTS</p>
#         </td>
# 	</tr>
# 	<tr>
# 		<td width="76"></td>
# 		<td colspan="10" bgcolor="#f6f6f6" style="border-radius:40px;">
# 			<div class="alignment" align="center" style="line-height:10px; margin-top:35px;"><img src="images/icon_dollor_up.png" style="display: block; height: 111px; border: 0; width: 60px; max-width: 100%;" width="60px" alt="Icon dollor up" title="Icon dollor up"></div>
#             <div style="font-family:Arial Narrow, Helvetica, sans-serif;">
# 				<p style="font-size:26px; color:#1A7CAB; font-weight:bold; margin-top:20px; padding-top:15px; padding-bottom:5px; margin-bottom:10px; text-align: center;"><span><strong>EXPENSE</strong></span></p>
# 			</div>
#             <div style="font-family: Arial Narrow, Helvetica, sans-serif;">
#         		<p style="margin: 0; text-align: center; font-size: 20px; color:#282828; padding-left:20px; padding-right:20px; padding-top:5px;"><strong>{}</strong></p>
# 			</div>
#             <div style="font-family: Arial Narrow, Helvetica, sans-serif; text-align:center; margin-top:15px;"><img align="center" alt="graph" height="347" src="images/a.png" width="484"/></div>
#             <div class="alignment" align="center"><a href="www.example.com" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top:5px;padding-bottom:5px; margin-bottom:35px; font-family:Arial Narrow, Helvetica, sans-serif; text-align:center; margin-top:32px;"><span style="padding-left:25px;padding-right:25px;font-size:13px;display:inline-block;"><span dir="ltr" style="word-break: break-word;"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">DETAILS</span></span></span></a>
# 			<!--<p style="    color: darkgray;
#     text-align: center;
#     font-size: 14px;
#     margin-bottom: 37px;
# ">Note: Upward/Downward arrow indicates percentage increase/decrease compared to last year</p>-->
# 		</div>
#         </td>
# 		<td width="76"></td>
# 	</tr>
# 	<tr>
# 		<td colspan="12">
# 			<p style="margin-top:25px; margin-bottom:35px; margin-left:25px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:24px; font-weight:bold; color:#1A7CAB;">WERE THESE INSIGHTS USEFUL ?</p>
#         </td>
# 	</tr>
# 	<tr>
# 		<td colspan="2">
# 			<img src="images/mailer_4seeInsights2_10.jpg" width="112" height="81" alt=""></td>
# 		<td colspan="3">
# 			<a href="#" target="_blank"><img src="images/mailer_4seeInsights2_11.jpg" width="174" height="81" alt=""></a></td>
# 		<td>
# 			<img src="images/mailer_4seeInsights2_12.jpg" width="138" height="81" alt=""></td>
# 		<td colspan="4">
# 			<a href="#" target="_blank"><img src="images/mailer_4seeInsights2_13.jpg" width="174" height="81" alt=""></a></td>
# 		<td colspan="2">
# 			<img src="images/mailer_4seeInsights2_14.jpg" width="82" height="81" alt=""></td>
# 	</tr>
# 	<tr>
# 		<td colspan="12">
# 			<img src="images/mailer_4seeInsights2_15.jpg" width="680" height="60" alt=""></td>
# 	</tr>
# 	<tr>
# 		<td colspan="3">
# 			<img src="images/mailer_4seeInsights2_16.jpg" width="207" height="69" alt=""></td>
# 		<td colspan="5">
# 			<a href="#" target="_blank"><img src="images/mailer_4seeInsights2_17.jpg" width="267" height="69" alt=""></a></td>
# 		<td width="45" height="69"></td>
# 		<td colspan="3" width="161" height="69"></td>
# 	</tr>
# 	<tr>
# 		<td colspan="12">
# 			<p style="margin-left:45px; margin-right:45px; margin-bottom:10px; margin-top:35px; text-align:left; font-family:Arial, Helvetica, sans-serif; font-size:11px; color:#758c9d;">© 2022 The Annet Group Inc. All rights reserved. 4see Analytics, Retransform and the associated logos are registered trademarks of The Annet Group Inc.. All other trademarks are the property of their respective owners. Log in to  manage all your notification preferences in <a href="" target="_blank" style="text-decoration:none;">Email Settings</a>. This email was sent to <<a href="" target="_blank" style="text-decoration:none;">username email</a>> and is intended for that recipient. You've been sent this as you've chosen to receive our updates.</p>
#       <p style="margin-left:45px; margin-right:45px; margin-bottom:10px; text-align:left; font-family:Arial, Helvetica, sans-serif; font-size:11px; color:#758c9d;">Retransform Lts, 22 Bishopsgate London EC2N 4BQ. | The Annet Group Dallas 6330 Lyndon B. Johnson Fwy.Suite 234-A Dallas, TX 75240 | The Annet Group Asia D204, Dubai Silicon Oasis Headquarter Bldg, P.O. Box 341472, Dubai Silicon Oasis, Dubai, U.A.E | The Annet Group Oceania Level 1, 207-209, Marrickville Road, Marrickville NSW 2204, Australia | The Annet Group Central Asia Evergreen Industrial Estate, Shakti Mills Lane, Mahalaxmi, Mumbai 400011, India </p>
#         </td>
# 	</tr>
# 	<tr>
# 		<td colspan="12">
# 			<p style="margin-top:25px; margin-bottom:150px; margin-left:16px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:14px; font-weight:bold; color:#96ae47;"><a style="text-decoration:none; color:#96ae47;" href="" target="_blank">UNSUBSCRIBE</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">EMAIL PREFERENCES</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">PRIVACY POLICY</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">CONTACT US</a> | <a style="text-decoration:none; color:#96ae47;" href="" target="_blank">HELP</a></p>
#         </td>
# 	</tr>
# 	<tr>
# 		<td>
# 			<img src="images/spacer.gif" width="76" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="36" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="95" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="48" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="31" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="138" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="3" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="47" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="45" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="79" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="6" height="1" alt=""></td>
# 		<td>
# 			<img src="images/spacer.gif" width="76" height="1" alt=""></td>
# 	</tr>
# </table>
# <!-- End Save for Web Slices -->
# </body>
# </html>
# '''
#     return html_content
#