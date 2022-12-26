
//****************************************
// AWS S3Bucket Implementation | Ver 1.0
//****************************************
// Get S3Buckets List
// Create new S3Bucket
// Upload and download files
// Get list of all files
// Delete files
// Devloper : Mr. Ramesh K. 
//****************************************

using System;
using Amazon;
using Amazon.S3;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using System.Threading.Tasks;
using Adoreal.Api.Interface;
using Microsoft.AspNetCore.Http;
using System.Collections.Generic;
using Adoreal.Service.Models.SecretManages;
using Microsoft.Extensions.Configuration;

namespace Adoreal.Service.s3bucket
{
    public class S3Upload : IS3Upload
    {
        private readonly ISecreteManager _secreteManager;
        static string S3BucketName = String.Empty;
        static string AccessKey = String.Empty;
        static string SecretKey = String.Empty;
        public readonly string BaseCloudFrontUri;
        public readonly IHttpContextAccessor _context;
        public static IAmazonS3 client { get; set; }
        public S3Upload(ISecreteManager secreteManager, IHttpContextAccessor context)
        {
            _secreteManager = secreteManager;
            //var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetSection("S3Bucket");
            var AWSs3config = _secreteManager.GetAWSs3Secret().GetAwaiter().GetResult();
            var config = JsonConvert.DeserializeObject<AWSS3ConfigModel>(AWSs3config);
            S3BucketName = config.S3BucketName;
            AccessKey = config.S3AccessKey;
            SecretKey = config.S3SecretKey;
            BaseCloudFrontUri = config.CloudfrontUri;
            _context = context;
        }

        /// Buckets Listing
        public async void ListingBuckets()
        {
            try
            {
                client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);
                ListBucketsResponse response = await client.ListBucketsAsync();
                foreach (S3Bucket bucket in response.Buckets)
                {
                    Console.WriteLine("You own Bucket with name: {0}", bucket.BucketName);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An Error, number {0}, occurred when listing buckets with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
                }
            }
        }
        
        /// <summary>
        /// Check Bucket exist or not
        /// </summary>
        /// <param name="BucketName">Pass bucket name as string</param>
        public async void ExistBucket(string BucketName)
        {
            try
            {
                client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);

                await client.EnsureBucketExistsAsync(BucketName);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An Error, number {0}, occurred when creating a bucket with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Create new bucket
        /// </summary>
        /// <param name="bucketName">Pass bucket name to create a new bucket.</param>
        /// <returns></returns>
        public async Task<bool> CreateBucketAsync(string bucketName)
        {
            try
            {
                client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);
                Console.WriteLine("Creating Amazon S3 bucket...");
                var bucketExists = await AmazonS3Util.DoesS3BucketExistV2Async(client, S3BucketName);
                if (bucketExists)
                {
                    Console.WriteLine($"Amazon S3 bucket with name '{S3BucketName}' already exists");
                    return true;
                    //return false;
                }

                var bucketRequest = new PutBucketRequest()
                {
                    BucketName = bucketName,
                    UseClientRegion = true
                };

                var response = await client.PutBucketAsync(bucketRequest);

                if (response.HttpStatusCode != HttpStatusCode.OK)
                {
                    //Console.WriteLine("Something went wrong while creating AWS S3 bucket.", response);
                    return false;
                }

                Console.WriteLine("Amazon S3 bucket created successfully");
                return true;
            }
            catch (AmazonS3Exception)
            {
                //Console.WriteLine("Something went wrong", ex);
                throw;
            }
        }

        /// <summary>
        /// Create new bucket
        /// </summary>
        /// <param name="BucketName">Pass bucket name to create a new bucket.</param>
        public async void CreateABucket(string BucketName)
        {
            try
            {
                var request = new PutBucketRequest
                {
                    BucketName = BucketName
                };

                await client.PutBucketAsync(request);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An Error, number {0}, occurred when creating a bucket with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
                }
            }
        }


        /// <summary>
        /// Writing an object
        /// </summary>
        /// <param name="file"></param>
        /// <param name="keyName"></param>
        /// <returns></returns>
        public async Task<string> WritingAnObject(IFormFile file, string keyName)
        {
            try
            {
                using (var client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1))
                {
                    using (var newMemoryStream = new MemoryStream())
                    {
                        file.CopyTo(newMemoryStream);
                        newMemoryStream.Seek(0, SeekOrigin.Begin);
                        var uploadRequest = new TransferUtilityUploadRequest
                        {
                            InputStream = newMemoryStream,
                            Key = keyName, // filename extension.ToLower()
                            BucketName = S3BucketName // bucket name of S3
                        };
                        var fileTransferUtility = new TransferUtility(client);
                        await fileTransferUtility.UploadAsync(uploadRequest);
                    }
                    var url = await GeneratePreSignedURL(keyName);
                    return url;
                }
                //
                //await WritingAnObjectAsync(FileStream, bucketName, keyName, contentBody, filePath, contentType, metaTitle);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when writing an object", amazonS3Exception.Message);
                }
            }
            return null;
        }

        /// <summary>
        /// Cloud front URI creation
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public async Task<string> CloudFrontUri(string Key)
        {
            try
            {
                string ImageSize = null;
                if (_context.HttpContext != null)
                    ImageSize = _context.HttpContext.Request.Headers["imagesize"];
                if (!string.IsNullOrEmpty(Key))
                {
                    string url = String.Empty;
                    if (!string.IsNullOrEmpty(ImageSize))
                    {
                        url = BaseCloudFrontUri + "fit-in/" + Convert.ToString(ImageSize) + '/' + Key;
                    }
                    else
                    {
                        url = BaseCloudFrontUri + Key;
                    }

                    return Convert.ToString(url);
                }
            }
            catch (Exception ex)
            {
                return null;
            }

            return null;
        }

        /// <summary>
        /// Generate preSigned URL
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public async Task<string> GeneratePreSignedURL(string Key)
        {
            client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);

            var request = new GetPreSignedUrlRequest
            {
                BucketName = S3BucketName,
                Key = Key,
                Verb = HttpVerb.GET,
                Expires = DateTime.UtcNow.AddDays(7)
            };

            string url = client.GetPreSignedURL(request);
            return url;
        }

        /// <summary>
        /// Get files
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<GetObjectResponse> GetFile(string key)
        {
            var client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);

            GetObjectResponse response = await client.GetObjectAsync(S3BucketName, key);
            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                return response;
            else
                return null;
        }

        /// <summary>
        /// Reading an Object
        /// </summary>
        /// <param name="KeyName"></param>
        public async void ReadingAnObject(string KeyName)
        {
            try
            {
                var url = await GeneratePreSignedURL(KeyName);
                GetObjectRequest request = new GetObjectRequest()
                {
                    BucketName = S3BucketName,
                    Key = KeyName
                };
                var imgname = System.IO.Path.GetFileName(KeyName);
                using (GetObjectResponse response = await client.GetObjectAsync(request))
                {
                    string title = response.Metadata["x-amz-meta-title"];
                    Console.WriteLine("The object's title is {0}", title);
                    string dest = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), imgname);
                    //    string dest = Path.Combine(@"D:\testconsole", imgname);
                    if (!File.Exists(dest))
                    {
                        await response.WriteResponseStreamToFileAsync(dest, true, System.Threading.CancellationToken.None);
                    }

                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object", amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Listing files of Particular bucket
        /// </summary>
        /// <param name="BucketName"></param>
        public async void ListingObjects(string BucketName)
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = BucketName;
                ListObjectsResponse response = await client.ListObjectsAsync(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }

                // list only things starting with "foo"
                request.Prefix = "foo";
                response = await client.ListObjectsAsync(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }

                // list only things that come after "bar" alphabetically
                request.Prefix = null;
                request.Marker = "bar";
                response = await client.ListObjectsAsync(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }

                // only list 3 things
                request.Prefix = null;
                request.Marker = null;
                request.MaxKeys = 3;
                response = await client.ListObjectsAsync(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when listing objects", amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Deleting Existing File from S3Bucket
        /// </summary>
        /// <param name="KeyName"></param>
        public async void DeletingExistingFile(string KeyName)

        {
            try
            {
                client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);
                var fileTransferUtility = new TransferUtility(client);
                await fileTransferUtility.S3Client.DeleteObjectAsync(new DeleteObjectRequest()
                {
                    BucketName = S3BucketName,//S3BucketName,
                    Key = KeyName,//KeyName
                });
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when deleting an object", amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Download file from S3Bucket
        /// </summary>
        /// <param name="KeyName"></param>
        /// <returns></returns>
        public async Task<byte[]> DownloadFileAsync(string KeyName)
        {
            MemoryStream ms = null;
            try
            {
                GetObjectRequest getObjectRequest = new GetObjectRequest
                {
                    BucketName = S3BucketName,
                    Key = KeyName
                };

                client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUCentral1);
                using (var response = await client.GetObjectAsync(getObjectRequest))
                {
                    if (response.HttpStatusCode == HttpStatusCode.OK)
                    {
                        using (ms = new MemoryStream())
                        {
                            await response.ResponseStream.CopyToAsync(ms);
                        }
                    }
                }

                if (ms is null || ms.ToArray().Length < 1)
                    throw new FileNotFoundException(string.Format("The document '{0}' is not found", KeyName));

                return ms.ToArray();
            }
            catch (Exception)
            {
                throw;
            }
        }

    }
}
