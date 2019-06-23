package com.azure.microsoft;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class S3toBlob 
	{
	
	private static final String amazonObjectUrl = "Object url to be enter here";

	  private static final String azureBlobContainerName = "mytestcontainer"; 
	  private static final String azureBlobName = "myTestFile.csv";
	  private static String storageConnectionString = "" ;

	  
	public static void main(String[] args)throws URISyntaxException,StorageException,InvalidKeyException,FileNotFoundException,IOException
		{
			
		try

	      {

	           // Retrieve storage account from connection-string

	           CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

	           CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

	           CloudBlobContainer container = blobClient.getContainerReference(azureBlobContainerName);

	 

	           // Create the container if it does not exist

	           container.createIfNotExists();                   

	           CloudBlockBlob blockBlob = container.getBlockBlobReference(azureBlobName);

	           System.out.println("Created a reference for block blob in Windows Azure....");

	           System.out.println("Blob Uri: " + blockBlob.getUri());

	           System.out.println("Now trying to initiate copy....");

	                    

	           OperationContext op = new OperationContext();                   

	           blockBlob.startCopy(new URI(amazonObjectUrl), null, null, null,op);

	                    

	           System.out.println("Copy started....");

	           System.out.println("Now tracking blob's copy progress....");

	           Date startTime = new Date();

	 

	           boolean continueLoop = true;

	                 

	           while (continueLoop)

	           {

	                System.out.println("");

	                System.out.println("Fetching lists of blobs in Azure blob container....");

	                Iterable<ListBlobItem> blobsList = container.listBlobs(null, true, EnumSet.of(BlobListingDetails.COPY), null, op);

	                for (ListBlobItem blob : blobsList)

	                {

	                   CloudBlockBlob tempBlockBlob = (CloudBlockBlob) blob;                    

	                   System.out.println("Name  blockblob in cast: "+ tempBlockBlob.getName().toString());

	                   System.out.println("URI                    : "+ tempBlockBlob.getUri().toString());

	                   if ((tempBlockBlob.getName()).equals(azureBlobName))

	                   {            

	                      CopyState copyStatus =  (CopyState) tempBlockBlob.getCopyState();                        

	                       

	                      System.out.println("Getting CopyState......");

	                      if (tempBlockBlob != null)

	                      {

	                         System.out.println("Status blob copy......" + tempBlockBlob.getCopyState().getStatus().toString());

	                         System.out.println("Total bytes..........." + tempBlockBlob.getCopyState().getTotalBytes());

	                         System.out.println("Total bytes to copy..." + tempBlockBlob.getCopyState().getBytesCopied());

	                         float percentComplete = 100*

	                                ( tempBlockBlob.getCopyState().getBytesCopied()).floatValue() /

	                                ( tempBlockBlob.getCopyState().getTotalBytes()).floatValue();

	                         System.out.print("Perc. byte copied......");

	                         System.out.format("%.2f\n", percentComplete);                          

	                          

	                          if (copyStatus.getStatus() != CopyStatus.PENDING)

	                          {

	                              continueLoop = false;

	                          }

	                      }

	                   }

	              }

	              System.out.println("==============================================");

	              Thread.sleep(1000);

	          }

	          Date endTime = new Date();        

	          long diffTime = endTime.getTime() - startTime.getTime();

	          long diffSeconds = diffTime / 1000 % 60;

	          long diffMinutes = diffTime / (60 * 1000) % 60;

	          long diffHours = diffTime / (60 * 60 * 1000) % 24;

	          long diffDays = diffTime / (24 * 60 * 60 * 1000);

	 

	          System.out.println("time transfer (D HH:mm:ss):  "+

	                   diffDays + " " +

	                   diffHours +":"+

	                   diffMinutes+":"+

	                   diffSeconds);

	          System.out.println("Press any key to terminate the program....");

	          System.in.read();                

	         } catch (StorageException storageException) {

	                   System.out.print("StorageException encountered: ");

	                   System.out.println(storageException.getMessage());

	                   System.exit(-1);

	         } catch (URISyntaxException uriSyntaxException) {

	                   System.out.print("URISyntaxException encountered: ");

	                   System.out.println(uriSyntaxException.getMessage());

	                   System.exit(-1);

	         } catch (Exception e) {

	                   System.out.print("Exception encountered: ");

	                   System.out.println(e.getMessage());

	                   System.exit(-1);

	        }

	  }
		
}	
