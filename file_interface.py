import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        os.chdir('files/')

    def list(self,params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def get(self,params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return None
            fp = open(f"{filename}",'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except Exception as e:
            return dict(status='ERROR',data=str(e))
            
    def upload(self,params=[]):
        try:
            if len(params) < 2:
                return dict(status='ERROR',data='Incomplete parameters. Need filename and base64 content')
                
            filename = params[0]
            file_content_b64 = params[1]
            
            # Decode the base64 content
            file_bytes = base64.b64decode(file_content_b64)
            
            # Write to file
            with open(filename, 'wb') as fp:
                fp.write(file_bytes)
                
            return dict(status='OK',data=f'File {filename} uploaded successfully')
        except Exception as e:
            return dict(status='ERROR',data=str(e))
    
    def delete(self,params=[]):
        try:
            if len(params) < 1:
                return dict(status='ERROR',data='Filename parameter required')
                
            filename = params[0]
            
            # Check if file exists
            if not os.path.exists(filename):
                return dict(status='ERROR',data=f'File {filename} not found')
            
            # Delete the file
            os.remove(filename)
            return dict(status='OK',data=f'File {filename} deleted successfully')
        except Exception as e:
            return dict(status='ERROR',data=str(e))


if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
    # Test upload and delete here if needed