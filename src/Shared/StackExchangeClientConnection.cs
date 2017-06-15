//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Web.SessionState;
using StackExchange.Redis;

namespace Microsoft.Web.Redis
{
    internal class StackExchangeClientConnection : IRedisClientConnection
    {

        ConnectionMultiplexer redisMultiplexer;
        IDatabase connection;
        ProviderConfiguration configuration;
        private RedisUtility redisUtility;

        public StackExchangeClientConnection(ProviderConfiguration configuration)
        {
            this.configuration = configuration;
            this.redisUtility = new RedisUtility(configuration);
            ConfigurationOptions configOption;

            // If connection string is given then use it otherwise use individual options
            if (!string.IsNullOrEmpty(configuration.ConnectionString))
            {
                configOption = ConfigurationOptions.Parse(configuration.ConnectionString);
                // Setting explicitly 'abortconnect' to false. It will overwrite customer provided value for 'abortconnect'
                // As it doesn't make sense to allow to customer to set it to true as we don't give them access to ConnectionMultiplexer
                // in case of failure customer can not create ConnectionMultiplexer so right choice is to automatically create it by providing AbortOnConnectFail = false
                configOption.AbortOnConnectFail = false;
            }
            else
            {
                configOption = new ConfigurationOptions();
                if (configuration.Port == 0)
                {
                    configOption.EndPoints.Add(configuration.Host);
                }
                else
                {
                    configOption.EndPoints.Add(configuration.Host + ":" + configuration.Port);
                }
                configOption.Password = configuration.AccessKey;
                configOption.Ssl = configuration.UseSsl;
                configOption.AbortOnConnectFail = false;

                if (configuration.ConnectionTimeoutInMilliSec != 0)
                {
                    configOption.ConnectTimeout = configuration.ConnectionTimeoutInMilliSec;
                }

                if (configuration.OperationTimeoutInMilliSec != 0)
                {
                    configOption.SyncTimeout = configuration.OperationTimeoutInMilliSec;
                }
            }
            if (LogUtility.logger == null)
            {
                redisMultiplexer = ConnectionMultiplexer.Connect(configOption);
            }
            else
            {
                redisMultiplexer = ConnectionMultiplexer.Connect(configOption, LogUtility.logger);
            }

            this.connection = redisMultiplexer.GetDatabase(configOption.DefaultDatabase ?? configuration.DatabaseId);
        }

        public IDatabase RealConnection
        {
            get { return connection; }
        }

        public async Task CloseAsync()
        {
            await redisMultiplexer.CloseAsync();
        }

        public async Task<bool> ExpiryAsync(string key, int timeInSeconds)
        {
            TimeSpan timeSpan = new TimeSpan(0, 0, timeInSeconds);
            RedisKey redisKey = key;
            return (bool) await RetryLogic(() => connection.KeyExpireAsync(redisKey, timeSpan));
        }

        public async Task<object> EvalAsync(string script, string[] keyArgs, object[] valueArgs)
        {
            RedisKey[] redisKeyArgs = new RedisKey[keyArgs.Length];
            RedisValue[] redisValueArgs = new RedisValue[valueArgs.Length];
            
            int i = 0;
            foreach (string key in keyArgs)
            {
                redisKeyArgs[i] = key;
                i++;
            }

            i = 0;
            foreach (object val in valueArgs)
            {
                if (val.GetType() == typeof(byte[]))
                {
                    // User data is always in bytes
                    redisValueArgs[i] = (byte[])val;
                }
                else
                {
                    // Internal data like session timeout and indexes are stored as strings
                    redisValueArgs[i] = val.ToString();
                }
                i++;
            }
            return await RetryLogic(() => connection.ScriptEvaluateAsync(script, redisKeyArgs, redisValueArgs));
        }

        private async Task<T> RetryForScriptNotFound<T>(Func<Task<T>> redisOperation)
        {
            try
            {
                return await redisOperation.Invoke();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("NOSCRIPT"))
                {
                    // Second call should pass if it was script not found issue
                    return await redisOperation.Invoke();
                }
                throw;
            }
        }

        /// <summary>
        /// If retry timout is provide than we will retry first time after 20 ms and after that every 1 sec till retry timout is expired or we get value.
        /// </summary>
        private async Task<T> RetryLogic<T>(Func<Task<T>> redisOperation)
        {
            int timeToSleepBeforeRetryInMiliseconds = 20;
            DateTime startTime = DateTime.Now;
            while (true)
            {
                try
                {
                    return await RetryForScriptNotFound(redisOperation);
                }
                catch (Exception)
                {
                    TimeSpan passedTime = DateTime.Now - startTime;
                    if (configuration.RetryTimeout < passedTime)
                    {
                        throw;
                    }
                    else
                    {
                        int remainingTimeout = (int)(configuration.RetryTimeout.TotalMilliseconds - passedTime.TotalMilliseconds);
                        // if remaining time is less than 1 sec than wait only for that much time and than give a last try
                        if (remainingTimeout < timeToSleepBeforeRetryInMiliseconds)
                        {
                            timeToSleepBeforeRetryInMiliseconds = remainingTimeout;
                        }
                    }

                    // First time try after 20 msec after that try after 1 second
                    await Task.Delay(timeToSleepBeforeRetryInMiliseconds);
                    timeToSleepBeforeRetryInMiliseconds = 1000;
                }
            }
        }

        public int GetSessionTimeout(object rowDataFromRedis)
        {
            RedisResult rowDataAsRedisResult = (RedisResult)rowDataFromRedis;
            RedisResult[] lockScriptReturnValueArray = (RedisResult[])rowDataAsRedisResult;
            Debug.Assert(lockScriptReturnValueArray != null);
            Debug.Assert(lockScriptReturnValueArray[2] != null);
            int sessionTimeout = (int)lockScriptReturnValueArray[2];
            if (sessionTimeout == -1)
            {
                sessionTimeout = (int) configuration.SessionTimeout.TotalSeconds;
            }
            // converting seconds to minutes
            sessionTimeout = sessionTimeout / 60;
            return sessionTimeout;
        }

        public bool IsLocked(object rowDataFromRedis)
        {
            RedisResult rowDataAsRedisResult = (RedisResult)rowDataFromRedis;
            RedisResult[] lockScriptReturnValueArray = (RedisResult[])rowDataAsRedisResult;
            Debug.Assert(lockScriptReturnValueArray != null);
            Debug.Assert(lockScriptReturnValueArray[3] != null);
            return (bool)lockScriptReturnValueArray[3];
        }

        public string GetLockId(object rowDataFromRedis)
        {
            RedisResult rowDataAsRedisResult = (RedisResult)rowDataFromRedis;
            RedisResult[] lockScriptReturnValueArray = (RedisResult[])rowDataAsRedisResult;
            Debug.Assert(lockScriptReturnValueArray != null);
            return (string)lockScriptReturnValueArray[0];
        }

        public ISessionStateItemCollection GetSessionData(object rowDataFromRedis)
        {
            RedisResult rowDataAsRedisResult = (RedisResult)rowDataFromRedis;
            RedisResult[] lockScriptReturnValueArray = (RedisResult[])rowDataAsRedisResult;
            Debug.Assert(lockScriptReturnValueArray != null);

            ChangeTrackingSessionStateItemCollection sessionData = null;
            if (lockScriptReturnValueArray.Length > 1 && lockScriptReturnValueArray[1] != null)
            {
                RedisResult[] data = (RedisResult[])lockScriptReturnValueArray[1];
                
                // LUA script returns data as object array so keys and values are store one after another
                // This list has to be even because it contains pair of <key, value> as {key, value, key, value}
                if (data != null && data.Length != 0 && data.Length % 2 == 0)
                {
                    sessionData = new ChangeTrackingSessionStateItemCollection(redisUtility);
                    // In every cycle of loop we are getting one pair of key value and putting it into session items
                    // thats why increment is by 2 because we want to move to next pair
                    for (int i = 0; (i + 1) < data.Length; i += 2)
                    {
                        string key = (string) data[i];
                        if (key != null)
                        {
                            sessionData.SetData(key, (byte[])data[i + 1]);
                        }
                    }
                }
            }
            return sessionData;
        }

        public async Task SetAsync(string key, byte[] data, DateTime utcExpiry)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = data;
            TimeSpan timeSpanForExpiry = utcExpiry - DateTime.UtcNow;
            await connection.StringSetAsync(redisKey, redisValue, timeSpanForExpiry);
        }

        public async Task<byte[]> GetAsync(string key)
        {
            RedisKey redisKey = key;
            RedisValue redisValue = await connection.StringGetAsync(redisKey);
            return (byte[]) redisValue;
        }

        public async Task RemoveAsync(string key)
        {
            RedisKey redisKey = key;
            await connection.KeyDeleteAsync(redisKey);
        }

        public byte[] GetOutputCacheDataFromResult(object rowDataFromRedis) 
        {
            RedisResult rowDataAsRedisResult = (RedisResult)rowDataFromRedis;
            return (byte[]) rowDataAsRedisResult;
        }
    }
}
