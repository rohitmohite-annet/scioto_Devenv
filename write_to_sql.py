import pandas as pd
import pyodbc

server = 'epsql-srv-scioto-4see.database.windows.net'
database = 'qasciotodb'
username = 'sciotosqladmin'
password = 'Ret$nQ2stkl21'
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

from datetime import date

# now = datetime.datetime.now()
# current_time =  now.strftime("%d-%m-%Y %H:%M:%S")

dict = {
    'InsightsName': 'National tenants with Highest Operating Expense',
    'Subject': 'Insight: National tenants with Highest Operating Expense',
    'Body': '''<html>
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
		<td colspan="12" style="text-align:center;">
			<img src="https://qascioto4seeapp.azurewebsites.net/public/vendor/images/logo/mailer_4seeInsights2_03.jpg" width="172" height="90" alt="" style="text-align: center; padding-right: 31px;"></a></td>
	</tr>
	<tr>
		<td colspan="12">
			<p style="margin-top:35px; margin-bottom:35px; text-align:center; font-family:Arial Narrow, Helvetica, sans-serif; font-size:34px; font-weight:bold; color:#1A7CAB;">INSIGHTS</p>
        </td>
	</tr>
	<tr>
		<td width="76"></td>
		<td colspan="10" bgcolor="#f6f6f6" style="border-radius:40px;">
			<div class="alignment" align="center" style="line-height:10px; margin-top:35px; margin-right:19px;"><img src="https://qascioto4seeapp.azurewebsites.net/public/vendor/images/logo/icon_dollor_up.png" style="display: block; height: 111px; border: 0; width: 60px; max-width: 100%;" width="60px" alt="Icon dollor up" title="Icon dollor up"></div>
            <div style="font-family:Arial Narrow, Helvetica, sans-serif;">
				<p style="font-size:26px; color:#1A7CAB; font-weight:bold; margin-top:20px; padding-top:15px; padding-bottom:5px; margin-bottom:10px; text-align: center;"><span><strong style="font-size:34px">{insight_title}</strong></span></p>
			</div>
            <div style="font-family: Arial Narrow, Helvetica, sans-serif;">
        		<p style="margin: 0; text-align: center; font-size: 20px; color:#282828; padding-left:20px; padding-right:20px; padding-top:5px;"><strong style="font-size:24px">{insight_message}</strong></p>
			</div>
            <div style="font-family: Arial Narrow, Helvetica, sans-serif; text-align:center; margin-top:15px;"><img style="padding-left: 49px;"align="center" alt="graph" height="347" src="data:image/png;base64,{insight_graph}" width="484"/></div>
            <div class="alignment" align="center"><a href="https://qascioto4seeapp.azurewebsites.net/income-statement" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top:5px;padding-bottom:5px; margin-bottom:35px; font-family:Arial Narrow, Helvetica, sans-serif; font-weight:600; text-align:center; margin-top:16px;"><span style="padding-left:25px;padding-right:25px;font-size:15px;display:inline-block;"><span dir="ltr" style="word-break: break-word;"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">DETAILS</span></span></span></a>
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
		<td colspan="6"> 
			<div class="alignment" align="center" style="
    float: left;
    padding-left: 201px;
"><a href="https://qascioto4seeapp.azurewebsites.net/income-statement" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top: 10px;padding-bottom: 10px;margin-bottom:35px;font-family:Arial Narrow, Helvetica, sans-serif;text-align:center;margin-top:0;"><span style="padding-left: 45px;padding-right: 45px;font-size: 20px;display:inline-block;font-weight: bold;"><span dir="ltr" style="/ word-break: break-word; /"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">YES</span></span></span></a>
		</div>
        </td>

		<td colspan="4" style="">
			<div class="alignment" align="center" style="
    float: right;
    padding-right: 130px;
"><a href="https://qascioto4seeapp.azurewebsites.net/income-statement" target="_blank" style="text-decoration:none;display:inline-block;color:#ffffff;background-color:#96ae47;border-radius:40px;width:auto;padding-top: 10px;padding-bottom: 10px;margin-bottom:35px;font-family:Arial Narrow, Helvetica, sans-serif;text-align:center;margin-top:0;"><span style="padding-left: 45px;padding-right: 45px;font-size: 20px;display:inline-block;font-weight: bold;"><span dir="ltr" style="/ word-break: break-word; /"><span style="line-height: 20px; padding-left:15px; padding-right:15px;">NO</span></span></span></a>
		</div>	
        </td>
	</tr>
	<tr>
		<td colspan="12" style="padding-bottom: 30px; padding-left: 40px; padding-right: 40px;">
			<hr></hr></td>
	</tr>
	<tr>

		<td colspan="12" style="text-align: center;">
			<img src="https://qascioto4seeapp.azurewebsites.net/public/vendor/images/logo/mailer_4seeInsights2_17.jpg" width="267" height="69" alt="" style="
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
''',
    'EmailTOAddress': 'siddhi.utekar@annet.com',
    'EmailCCAddress': 'rohit.mohite@annet.com,siddhi.utekar@annet.com,nilesh.thote@annet.com',
    'CreatedBy': '',

}
dataframe_1 = pd.DataFrame([dict], columns=dict.keys())
# dataframe_1.to_sql("EmailTemplateMasterInsights", schema='dbo',cnxn)

import sqlalchemy
import pyodbc
from sqlalchemy import create_engine
import urllib

quoted = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
# # write the DataFrame to a table in the sql database
dataframe_1.to_sql('EmailTemplateMasterInsights', schema='dbo', con = engine,if_exists='append',index = False)