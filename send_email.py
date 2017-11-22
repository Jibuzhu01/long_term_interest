# coding=gbk

import sys
import urllib
import urllib2


body = sys.argv[1]
fr_address = "ouyuanbiao@sogou-inc.com"
uid = "ouyuanbiao@sogou-inc.com"
fr_name = "OuYuanBiao"
title = "[ERROR] long term interest"
mode = "html"
maillist="yuanjun@sogou-inc.com"
if len(sys.argv) >= 3 and sys.argv[2] == "1":
    maillist = "yuanjun@sogou-inc.com"
    
attname = "test_attname"
attbody = "test_attbody"


def send_email_post():
	url = "http://mail.portal.sogou/portal/tools/send_mail.php"
	data = {"uid": uid, "fr_name": fr_name, "fr_address": fr_address, "title": title, "body": body, "mode": mode, "maillist": maillist, "attname": attname, "attbody": attbody}
	req = urllib2.Request(url, urllib.urlencode(data))
	resp = urllib2.urlopen(req)


def send_email_get():
	url = "http://mail.portal.sogou/portal/tools/send_mail.php"
	data = {"uid": uid, "fr_name": fr_name, "fr_address": fr_address, "title": title, "body": body, "mode": mode, "maillist": maillist, "attname": attname, "attbody": attbody}
	query = urllib.urlencode(data)
	resp = urllib2.urlopen(url + "?" + query)


if __name__ == "__main__":
	send_email_get()

