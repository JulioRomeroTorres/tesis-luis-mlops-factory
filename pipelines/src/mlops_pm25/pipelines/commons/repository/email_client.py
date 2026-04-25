from typing import List, Optional
from .http_client import HttpClient
from mlops_pm25.pipelines.commons.domain.constants import BASE_URL_EMAIL_SERVICE, ENDPOINT_NOTIFICATION_SERVICE, TypeAttachedFile

class EmailClient:
    def __init__(self, 
        _from: str,
        to: List[str],
        cc: List[str],
        html_body: str,
        subject: str,
        attached_files: Optional[List[str]] = None
    ):
        self._from = _from
        self.to = to
        self.cc = cc
        self.html_body =  html_body
        self.attached_files = attached_files
        self.http_client = HttpClient(
            BASE_URL_EMAIL_SERVICE
        )
        self.subject = subject

    def _generate_attached_file_structure(self):

        return [
            {
            "filename": attached_file.name_file,
            "type":  TypeAttachedFile.URL.value,
            "content": attached_file.path_file,
            }
            for attached_file in self.attached_files
        ] 
    def send_email(
        self
    ): 
        try:
            params = {
                "from": self._from,
                "fromName": "Notificaciones Rimac",
                "to": self.to,
                "cc": self.cc,
                "html": self.html_body,
                "subject": self.subject,
                "monitoreo": True
            }
            
            self.http_client.post(
                ENDPOINT_NOTIFICATION_SERVICE,
                params
            )
        except Exception as error:
            print("Error", error)
            raise error

        