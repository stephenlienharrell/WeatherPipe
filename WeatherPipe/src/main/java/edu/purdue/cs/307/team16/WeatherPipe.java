import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.util.cache.FileCacheable;

// goal: to be able to read data from s3

public class WeatherPipe {

	public static void main(String[] args){


		byte[] buffer = new byte[1024];

		AWSCredentials credentials = null;
		//try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		//} 
		/*catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/Users/lalavaishnode/.aws/credentials), and is in valid format.",
							e);
			
		}*/


		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usEast1);


		// Setting bucket parameters
		String bucketName = "noaa-nexrad-level2";
		String key = "2010/01/01/KDDC/KDDC20100101_073731_V03.gz";
		String file_output_stream = "F:\\CS\\307\\WeatherPipe\\objectgay.txt";


		try {
			// Download required object from S3
			System.out.println("Downloading an object");
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
			
			
				//displayTextInputStream(object.getObjectContent());
				
				
				/*BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		        while (true) {
		            String line = reader.readLine();
		            if (line == null) break;
		            System.out.println("    " + line);
		        }
		        System.out.println();
		    }*/
				

			try {
				// to unzip this gay object
				System.out.println("Unzipping gay object");
				
				
				GZIPInputStream gZIPInputStream = new GZIPInputStream(object.getObjectContent());

				FileOutputStream fileOutputStream = new FileOutputStream(file_output_stream);

				int bytes_read;

				while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {

					fileOutputStream.write(buffer, 0, bytes_read);

				}


				gZIPInputStream.close();

				fileOutputStream.close();


				System.out.println("The file was decompressed successfully!");



			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} 
		catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		
		
		
		//change the file into netcdf format
		try {
			NetcdfFile file = NetcdfFile.open(file_output_stream);
			System.out.println("Description: " + file.getFileTypeDescription());
			System.out.println("Cache Name: " + file.getCacheName());
			System.out.println("Detial Information: " + file.getDetailInfo());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		

	}
	
	
	private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }
	
	
}
