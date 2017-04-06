# coding: utf-8
#!/usr/bin/env python

import os

class EmailComposer:
    '''
    composes emails to receipients

    '''
    def __init__(self, configItems, emailer):
        self._configItems =  configItems
        self._emailer = emailer
        self._emailer_configs = emailer._emailConfigs


    def getBaseMsgText(self, situation):
        msgBody = None
        fn = self._emailer_configs['email_situations'][situation]['email_txt_basedir'] + self._emailer_configs['email_situations'][situation]['text_file_main']
        with open(fn, 'r') as myfile:
            msgBody=myfile.read().replace('\n', '')
        return msgBody

    #def email_msg(self, subject_line, msgBody, attachment=None, attachment_fullpath=None, receipient=None):
    #    if os.path.isfile(attachment_fullpath):
    #        self._emailer.sendEmails(  subject_line, msgBody, attachment, attachment_fullpath, receipient, None, False)
    #    else:
    #        self._emailer.sendEmails(  subject_line, msgBody, None, None, receipient, None, False)

    @staticmethod
    def get_msgparts(email_txt_basedir, situation, text_file_subparts=None):
        msg_subpart_dict = {}
        for txt_file in text_file_subparts:
            msg_subpart_dict[txt_file] = EmailComposer.getMsgText(email_txt_basedir, situation, txt_file )
        return  msg_subpart_dict

if __name__ == "__main__":
    main()
