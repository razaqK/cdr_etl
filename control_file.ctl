OPTIONS 
( ERRORS={errors}, BINDSIZE={bindsize}, ROWS={rows}, READSIZE={readsize},PARALLEL={parallel},DIRECT={direct})
 UNRECOVERABLE LOAD DATA
 CHARACTERSET WE8MSWIN1252
 INFILE {infile}
 BADFILE {badfile}
 INTO TABLE ldr_prepaid_voice_cdr_01
  APPEND
  REENABLE DISABLED_CONSTRAINTS
  FIELDS
    TERMINATED BY '|'
    TRAILING NULLCOLS
  (
 FILENAME, FILEDATE, SERIALNO, SUBSEQUENCE, TIMESTAMP, SERVICEKEY, CALLINGPARTYNUMBER, CALLEDPARTYNUMBER, 
CALLINGPARTYIMSI, CALLEDPARTYIMSI, DIALEDNUMBER, ORIGINALCALLEDPARTY, SERVICEFLOW, CALLFORWARDINDICATOR, 
CALLINGROAMINFO, CALLINGCELLID, CALLEDROAMINFO, CALLEDCELLID, TIMESTAMPOFSSP, TIMEZONEOFSSP, BEAREDSERVICE,
RESERVED01, CHARGINGTIME, WAITDURATION, CALLDURATION, TERMINATIONREASON, CALLREFERENCENUMBER, IMEI, 
CHARGEDURATION, ACCESSPREFIX, ROUTINGPREFIX, REDIRECTINGPARTYID, BRANDID, SUBCOSID, CHARGINGPARTYNUMBER, 
CHARGEPARTYINDICATOR, PAYTYPE, BILLCYCLEID, CHARGINGTYPE, CALLTYPE, ROAMSTATE, RESULTCODE, FPHPREFIX, 
CALLINGHOMECOUNTRYCODE, CALLINGHOMEAREANUMBER, CALLINGHOMENETWORKCODE, CALLINGROAMCOUNTRYCODE, CALLINGROAMAREANUMBER, 
CALLINGROAMNETWORKCODE, CALLEDHOMECOUNTRYCODE, CALLEDHOMEAREANUMBER, CALLEDHOMENETWORKCODE, CALLEDROAMCOUNTRYCODE, 
CALLEDROAMAREANUMBER, CALLEDROAMNETWORKCODE, PRODUCTID, SERVICETYPE, HOTLINEINDICATOR, HOMEZONEID, NPFLAG, NPPREFIX, 
CALLINGGROUPNO, CALLEDGROUPNO, USERSTATE, SUBSCRIBERID, OPPOSENETWORKTYPE, RESERVED02, CHARGEOFITEMSACCOUNTS, 
CHARGEOFDURATIONACCOUNTS, CHARGEOFFUNDACCOUNTS, CHARGEFROMPREPAID, PREPAIDBALANCE, CHARGEFROMPOSTPAID, POSTPAIDBALANCE, 
ACCOUNTID, CURRENCYCODE, RESERVED03, ACCOUNTTYPE1, FEETYPE1, CHARGEAMOUNT1, CURRENTACCTAMOUNT1, ACCOUNTTYPE2, FEETYPE2, 
CHARGEAMOUNT2, CURRENTACCTAMOUNT2, ACCOUNTTYPE3, FEETYPE3, CHARGEAMOUNT3, CURRENTACCTAMOUNT3, ACCOUNTTYPE4, FEETYPE4, 
CHARGEAMOUNT4, CURRENTACCTAMOUNT4, ACCOUNTTYPE5, FEETYPE5, CHARGEAMOUNT5, CURRENTACCTAMOUNT5, ACCOUNTTYPE6, FEETYPE6, 
CHARGEAMOUNT6, CURRENTACCTAMOUNT6, ACCOUNTTYPE7, FEETYPE7, CHARGEAMOUNT7, CURRENTACCTAMOUNT7, ACCOUNTTYPE8, FEETYPE8, 
CHARGEAMOUNT8, CURRENTACCTAMOUNT8, ACCOUNTTYPE9, FEETYPE9, CHARGEAMOUNT9, CURRENTACCTAMOUNT9, ACCOUNTTYPE10, FEETYPE10, 
CHARGEAMOUNT10, CURRENTACCTAMOUNT10, BONUSVALIDITY1, BONUSVALIDITY2, BONUSVALIDITY3, BONUSVALIDITY4, BONUSVALIDITY5, 
BONUSVALIDITY6, BONUSVALIDITY7, BONUSVALIDITY8, BONUSVALIDITY9, BONUSVALIDITY10, VPNGROUPID, CALLINGVPNGROUPNUMBER, 
CALLINGVPNSHORTNUMBER, CALLEDVPNGROUPNUMBER, CALLEDVPNSHORTNUMBER, CALLINGNETWORKTYPE, CALLEDNETWORKTYPE, GROUPCALLTYPE, 
GROUPPAYFLAG, RESERVED04, RESERVED05, RESERVED06, RESERVED07, RESERVED08, ADDTIONAINFO, STARTTIMEOFBILLCYCLE
   )

