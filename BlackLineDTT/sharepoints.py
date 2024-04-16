from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from concurrent.futures import ThreadPoolExecutor, as_completed

class SharepointUtility:
    def __init__(self, sharepoint_url,connection_type,id=None,secret=None,username=None,password=None):
        self.sharepoint_url=sharepoint_url
        if connection_type == "user":
            self.ctx=self.get_sharepoint_context_using_user(username,password)
        else:
            self.ctx=self.get_sharepoint_context_using_app(id,secret)
            
    def get_sharepoint_context_using_app(self,id,secret):
            # Initialize the client credentials
        client_credentials = ClientCredential(id,secret)
        # create client context object
        ctx = ClientContext(self.sharepoint_url).with_credentials(client_credentials)
        return ctx

    def get_sharepoint_context_using_user(self,username,password):
        # Initialize the client credentials
        user_credentials = UserCredential(username,password)
        # create client context object
        ctx = ClientContext(self.sharepoint_url).with_credentials(user_credentials)
        return ctx

    def create_sharepoint_directory(self,dir_name: str):
        """
        Creates a folder in the sharepoint directory.
        """
        if dir_name:
            result = self.ctx.web.folders.add(f'Shared Documents/{dir_name}').execute_query()
            if result:
                # documents is titled as Shared Documents for relative URL in SP
                relative_url = f'Shared Documents/{dir_name}'
                return relative_url

    def upload_to_sharepoint_multiple(self,dir_name: str, upload_file_lst: list):
        sp_relative_url = self.create_sharepoint_directory(dir_name)
        target_folder = self.ctx.web.get_folder_by_server_relative_url(sp_relative_url)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for file_name in upload_file_lst:
                with open(file_name, 'rb') as content_file:
                    file_content = content_file.read()
                    futures = executor.submit(target_folder.upload_file, file_name, file_content)
                    if as_completed(futures):
                        futures.result().execute_query()

    def upload_to_sharepoint_single(self,dir_name: str, file_name: str):

        sp_relative_url = self.create_sharepoint_directory(dir_name)
        target_folder = self.ctx.web.get_folder_by_server_relative_url(sp_relative_url)
        with open(file_name, 'rb') as content_file:
            file_content = content_file.read()
            target_folder.upload_file(file_name, file_content).execute_query()



