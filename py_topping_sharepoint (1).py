from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext, UserCredential
from office365.sharepoint.files.file import File
import os, csv, requests
from io import BytesIO
import pandas as pd
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.http.http_method import HttpMethod
import json

class da_tran_SP_PRIM:
    def __init__(self, user,password) :
        self.user = user
        self.password = password

    def download(self,download_url,local_location):
        resp=requests.post(download_url, auth=requests.auth.HTTPBasicAuth(self.user, self.password))
        with open(local_location, 'wb') as output :
           output.write(resp.content)

    def upload(self, local_location, sharepoint_url, sharepoint_folder, sharepoint_file_name = ''):
        with open(local_location, 'rb') as file :
            if sharepoint_file_name == '' : sharepoint_file_name = local_location
            #Sets up the url for requesting a file upload
            requestUrl = sharepoint_url + '/_api/web/getfolderbyserverrelativeurl(\'' + sharepoint_folder + '\')/Files/add(url=\'' + sharepoint_file_name + '\',overwrite=true)'
            #Setup the required headers for communicating with SharePoint 
            headers = {'Content-Type': 'application/json; odata=verbose', 'accept': 'application/json;odata=verbose'}
            #Execute a request to get the FormDigestValue. This will be used to authenticate our upload request
            r = requests.post(sharepoint_url + "/_api/contextinfo",auth=requests.auth.HTTPBasicAuth(self.user, self.password), headers=headers)
            formDigestValue = r.json()['d']['GetContextWebInformation']['FormDigestValue']
            #Update headers to use the newly acquired FormDigestValue
            headers = {'Content-Type': 'application/json; odata=verbose', 'accept': 'application/json;odata=verbose', 'x-requestdigest' : formDigestValue}
            #Execute the request. If you run into issues, inspect the contents of uploadResult
            requests.post(requestUrl,auth=requests.auth.HTTPBasicAuth(self.user, self.password), data=file.read(),headers=headers)
    
class da_tran_SP365:
    def __init__(self, site_url, client_id = '', client_secret = '', user = '' , password = ''):
        """Create connection to Sharepoint Site"""
        self.site_url = site_url
        if (client_id != '') & (client_secret != '') :
            self.client_id = client_id
            self.client_secret = client_secret
            ctx_auth = AuthenticationContext( self.site_url)
            ctx_auth.acquire_token_for_app( self.client_id , self.client_secret )
        elif (user != '') & (password != '') :
            self.user = user
            user_credentials = UserCredential(user,password)
            ctx_auth = ClientContext(self.site_url).with_credentials(user_credentials)
        else : raise Exception("Please Insert parameters : 'client_id' and 'client_secret' or 'user' and 'password'")
        self.ctx = ClientContext(self.site_url, ctx_auth)
        web = self.ctx.web
        self.ctx.load(web)
        self.ctx.execute_query()
        #print('Authen OK')

    def create_link(self, file_link):
        file_name = file_link.split('/')[-1]
        link = '/sites' + file_link.replace(file_name,'').split('sites')[-1]
        link += file_name.split('?')[0]
        return link

    def download(self, sharepoint_location, local_location = '', as_dataframe = False, sheet_name = None) :
        """Download file from sharepoint or Read Excel/csv from sharepoint as pd dataframe"""
        response = File.open_binary(self.ctx, sharepoint_location ) # save file from sharepoint as binary
        if str(response.status_code) == '200' :
            if as_dataframe :
                toread = BytesIO()
                toread.write(response.content)  # pass your `decrypted` string as the argument here
                toread.seek(0)  # reset the pointer
                if '.csv' in sharepoint_location : return pd.read_csv(toread)
                else : return pd.read_excel(toread, sheet_name = sheet_name)
            else :
                with open(local_location, "wb") as local_file: 
                    local_file.write(response.content) # write in your pc
                #print('Download OK File')
        else :
            raise Exception('Cannot Download File', response)

    def upload(self, sharepoint_location, local_location):
        """Upload file to sharepoint"""
        with open(local_location, 'rb') as content_file: 
            file_content = content_file.read() # read file from your pc
        dir_, name = os.path.split(sharepoint_location)
        self.ctx.web.get_folder_by_server_relative_url(dir_).upload_file(name, file_content).execute_query() # upload file to sharepoint

    def read_list(self, list_title, local_location = '', as_dataframe = False):
        """Read list from Sharepoint and Download as csv or pandas dataframe"""
        list_to_export = self.ctx.web.lists.get_by_title(list_title)
        list_items = list_to_export.items.get().execute_query()
        self.list_items = list_items
        if len(list_items) == 0: print("No data found")
        else :
            if as_dataframe :
                df_in = pd.DataFrame()
                for i in list_items : df_in = df_in.append(pd.DataFrame(data = [list(i.properties.values())], columns = i.properties.keys()))
                return df_in
            elif local_location == '' : raise Exception("Please input your savefile's name")
            else :
                with open(local_location, 'w',newline='') as fh:
                    fields = list_items[0].properties.keys()
                    w = csv.DictWriter(fh, fields)
                    w.writeheader()
                    for item in list_items: w.writerow(item.properties)
                print('Download List OK')
                
#   Additions to the original py_topping library follow
#   @author Maximiliano Ariel LOPEZ
                
    def listFiles(self):
        url = r"{0}/_api/files".format(self.site_url)
    
        request = RequestOptions(url)
        request.method = HttpMethod.Get
        response = self.ctx.execute_request_direct(request)
        jsonResponse = json.loads(response.text)
        return jsonResponse['d']['results']
    
    def listFilesWithTimeStamp(self, searchPath):
        files = self.listFiles()
        result = []
        
        for file in files:
            if searchPath in file['Url']:
                result.append({'name': file['Url'], 'timestamp': file['TimeLastModified']})
                
        return result
    
    def latestFilename(self, searchPath):
        files = self.listFilesWithTimeStamp(searchPath)
        
        def getTimeStamp(obj):
            return obj['timestamp']
        
        files.sort(key=getTimeStamp, reverse=True)
        
        return files[0]['name']
    
    def latestFileSuffix(self, searchPath, timeStampLength=19, extensionLength=3):
        name = self.latestFilename(searchPath)
        
        vector = name.split("/")
        suffix = vector[len(vector)-1]
        
        return suffix[len(suffix)-(timeStampLength+extensionLength+1):-extensionLength-1]    
    
    def listLatestFiles(self, searchPath, timeStampLength=19, extensionLength=3):
        files = self.listFiles()
        latestSuffix = self.latestFileSuffix(searchPath, timeStampLength, extensionLength)
        print("Latest file set found:", latestSuffix)
        result = []
        
        for file in files:
            if searchPath in file['Url'] and latestSuffix in file['Url']:
                result.append(file['Url'])
                
        return result
    
    def deleteFilesFromFolder(self, site_url, folder, extension):
        files = self.listFiles()
        suffix = self.latestFileSuffix(folder)
        
        for f in files:
            file = f['Url']
            
            if folder not in file or extension not in file or suffix in file:
                continue
        
            filename_vector = file.split("/")
            filename = filename_vector[len(filename_vector)-1]
            url = site_url + "/_api/web/GetFileByServerRelativeUrl('/sites/" + site_url.split("/sites/")[1] + "/Shared%20Documents/" + folder + filename + "')"
            
            request = RequestOptions(url)
            
            request.method = HttpMethod.Post
            request.set_header('X-HTTP-Method', 'DELETE')
            request.set_header('IF-MATCH', '*')
            
            response = self.ctx.execute_request_direct(request)
        
            if str(response.status_code) == '200' :
                print('Deleted', file)
            else:
                raise Exception('Cannot delete', file, response)