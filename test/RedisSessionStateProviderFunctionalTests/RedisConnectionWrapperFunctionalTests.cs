//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using FakeItEasy;
using Microsoft.Web.Redis.Tests;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.SessionState;
using Xunit;
using System.Threading;
using Microsoft.AspNet.SessionState;

namespace Microsoft.Web.Redis.FunctionalTests
{
    public class RedisConnectionWrapperFunctionalTests
    {
        static int uniqueSessionNumber = 1;
        private static RedisUtility RedisUtility = new RedisUtility(Utility.GetDefaultConfigUtility());

        private RedisConnectionWrapper GetRedisConnectionWrapperWithUniqueSession()
        {
            
            return GetRedisConnectionWrapperWithUniqueSession(Utility.GetDefaultConfigUtility());
        }

        private RedisConnectionWrapper GetRedisConnectionWrapperWithUniqueSession(ProviderConfiguration pc)
        {
            string id = Guid.NewGuid().ToString();
            uniqueSessionNumber++;
            // Initial connection with redis
            RedisConnectionWrapper.sharedConnection = null;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(pc, id);
            return redisConn;
        }

        private async Task DisposeRedisConnectionWrapper(RedisConnectionWrapper redisConn)
        {
            await redisConn.redisConnection.CloseAsync();
            RedisConnectionWrapper.sharedConnection = null;
        }

        [Fact]
        public async Task Set_ValidData_WithCustomSerializer()
        {
            
            // this also tests host:port config part
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            pc.RedisSerializerType = typeof(TestSerializer).AssemblyQualifiedName;
            pc.ApplicationName = "APPTEST";
            pc.Port = 6379;
            RedisUtility testSerializerRedisUtility = new RedisUtility(pc);

            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession(pc);
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(testSerializerRedisUtility);
                data["key"] = "value";
                data["key1"] = "value1";
                await redisConn.SetAsync(data, 900);

                // Get actual connection and get data blob from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);

                // Check that data shoud be same as what inserted
                Assert.Equal(2, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection dataFromRedis = new ChangeTrackingSessionStateItemCollection(testSerializerRedisUtility);
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    dataFromRedis[entry.Name] = testSerializerRedisUtility.GetObjectFromBytes(entry.Value).ToString();
                }
                Assert.Equal("value", dataFromRedis["key"]);
                Assert.Equal("value1", dataFromRedis["key1"]);

                // remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task Set_ValidData()
        {
            // this also tests host:port config part
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            pc.ApplicationName = "APPTEST";
            pc.Port = 6379;

            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession(pc);

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                data["key1"] = "value1";
                await redisConn.SetAsync(data, 900);

                // Get actual connection and get data blob from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);

                // Check that data shoud be same as what inserted
                Assert.Equal(2, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection dataFromRedis = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    dataFromRedis[entry.Name] = RedisUtility.GetObjectFromBytes(entry.Value).ToString();
                }
                Assert.Equal("value", dataFromRedis["key"]);
                Assert.Equal("value1", dataFromRedis["key1"]);

                // remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task Set_NullData()
        {
            // this also tests host:port config part
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            pc.ApplicationName = "APPTEST";
            pc.Port = 6379;

            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession(pc);

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                data["key1"] = null;
                await redisConn.SetAsync(data, 900);

                // Get actual connection and get data blob from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);

                // Check that data shoud be same as what inserted
                Assert.Equal(2, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection dataFromRedis = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    dataFromRedis[entry.Name] = RedisUtility.GetObjectFromBytes(entry.Value);
                }
                Assert.Equal("value", dataFromRedis["key"]);
                Assert.Equal(null, dataFromRedis["key1"]);

                // remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task Set_ExpireData()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
                // Inserting data into redis server that expires after 1 second
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 1);

                // Wait for 2 seconds so that data will expire
                System.Threading.Thread.Sleep(1100);

                // Get actual connection and get data blob from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);

                // Check that data shoud not be there
                Assert.Equal(0, sessionDataFromRedis.Length);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_WithNullData()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = null;
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;

                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(1, dataFromRedis.SessionData.Count);
                Assert.Null(dataFromRedis.SessionData["key"]);

                // Get actual connection and get data lock from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                string lockValueFromRedis = actualConnection.StringGet(redisConn.Keys.LockKey);
                Assert.Equal(lockTime.Ticks.ToString(), lockValueFromRedis);

                // remove data and lock from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                actualConnection.KeyDelete(redisConn.Keys.LockKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_WriteLockWithoutAnyOtherLock()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());

                ChangeTrackingSessionStateItemCollection dataFromGet = (ChangeTrackingSessionStateItemCollection)dataFromRedis.SessionData;
                Assert.Null(((ValueWrapper)dataFromGet.innerCollection["key"]).GetActualValue());
                Assert.NotNull(((ValueWrapper)dataFromGet.innerCollection["key"]).GetSerializedvalue());
                Assert.Equal(1, dataFromRedis.SessionData.Count);

                // this will desirialize value
                Assert.Equal("value", dataFromRedis.SessionData["key"]);
                Assert.Equal("value", ((ValueWrapper)dataFromGet.innerCollection["key"]).GetActualValue());

                // Get actual connection and get data lock from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                string lockValueFromRedis = actualConnection.StringGet(redisConn.Keys.LockKey);
                Assert.Equal(lockTime.Ticks.ToString(), lockValueFromRedis);

                // remove data and lock from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                actualConnection.KeyDelete(redisConn.Keys.LockKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_WriteLockWithOtherWriteLock()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);
                
                // Takewrite lock successfully first time
                DateTime lockTime_1 = DateTime.Now;
                GetItemData dataFromRedis_1 = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime_1, 900);
                Assert.True(dataFromRedis_1.IsLockTaken);
                Assert.Equal(lockTime_1.Ticks.ToString(), dataFromRedis_1.LockId.ToString());
                Assert.Equal(1, dataFromRedis_1.SessionData.Count);

                // try to take write lock and fail and get earlier lock id
                DateTime lockTime_2 = lockTime_1.AddSeconds(1);
                GetItemData dataFromRedis_2 = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime_2, 900);
                Assert.False(dataFromRedis_2.IsLockTaken);
                Assert.Equal(lockTime_1.Ticks.ToString(), dataFromRedis_2.LockId.ToString());
                Assert.Equal(null, dataFromRedis_2.SessionData);

                // Get actual connection
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                // remove data and lock from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                actualConnection.KeyDelete(redisConn.Keys.LockKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_WriteLockWithOtherWriteLockWithSameLockId()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                // Same LockId 
                DateTime lockTime = DateTime.Now;

                // Takewrite lock successfully first time
                GetItemData dataFromRedis_1 = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis_1.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis_1.LockId.ToString());
                Assert.Equal(1, dataFromRedis_1.SessionData.Count);

                // try to take write lock and fail and get earlier lock id
                GetItemData dataFromRedis_2 = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.False(dataFromRedis_2.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis_2.LockId.ToString());
                Assert.Equal(null, dataFromRedis_2.SessionData);

                // Get actual connection
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                // remove data and lock from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                actualConnection.KeyDelete(redisConn.Keys.LockKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeReadLockAndGetData_WithoutAnyLock()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                GetItemData dataFromRedis = await redisConn.TryCheckWriteLockAndGetDataAsync();
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(null, dataFromRedis.LockId);
                Assert.Equal(1, dataFromRedis.SessionData.Count);
                Assert.Equal("value", dataFromRedis.SessionData["key"]);

                // Get actual connection
                // remove data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeReadLockAndGetData_WithOtherWriteLock()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);
                
                DateTime lockTime_1 = DateTime.Now;
                GetItemData dataFromRedis_1 = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime_1, 900);
                Assert.True(dataFromRedis_1.IsLockTaken);
                Assert.Equal(lockTime_1.Ticks.ToString(), dataFromRedis_1.LockId.ToString());
                Assert.Equal(1, dataFromRedis_1.SessionData.Count);

                GetItemData dataFromRedis_2 = await redisConn.TryCheckWriteLockAndGetDataAsync();
                Assert.False(dataFromRedis_2.IsLockTaken);
                Assert.Equal(lockTime_1.Ticks.ToString(), dataFromRedis_2.LockId.ToString());
                Assert.Equal(null, dataFromRedis_2.SessionData);

                // Get actual connection
                // remove data and lock from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                actualConnection.KeyDelete(redisConn.Keys.LockKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_ExpireWriteLock()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                int lockTimeout = 1;
                
                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, lockTimeout);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(1, dataFromRedis.SessionData.Count);

                // Wait for 2 seconds so that lock will expire
                System.Threading.Thread.Sleep(1100);

                // Get actual connection and check that lock do not exists
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                string lockValueFromRedis = actualConnection.StringGet(redisConn.Keys.LockKey);
                Assert.Equal(null, lockValueFromRedis);

                // remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryReleaseLockIfLockIdMatch_ValidWriteLockRelease()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(1, dataFromRedis.SessionData.Count);

                await redisConn.TryReleaseLockIfLockIdMatchAsync(dataFromRedis.LockId, 900);

                // Get actual connection and check that lock do not exists
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                string lockValueFromRedis = actualConnection.StringGet(redisConn.Keys.LockKey);
                Assert.Equal(null, lockValueFromRedis);

                // remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryReleaseLockIfLockIdMatch_InvalidWriteLockRelease()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(1, dataFromRedis.SessionData.Count);

                object wrongLockId = lockTime.AddSeconds(1).Ticks.ToString();
                await redisConn.TryReleaseLockIfLockIdMatchAsync(wrongLockId, 900);

                // Get actual connection and check that lock do not exists
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                string lockValueFromRedis = actualConnection.StringGet(redisConn.Keys.LockKey);
                Assert.Equal(dataFromRedis.LockId, lockValueFromRedis);

                // remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                actualConnection.KeyDelete(redisConn.Keys.LockKey);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryRemoveIfLockIdMatch_ValidLockIdAndRemove()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(1, dataFromRedis.SessionData.Count);

                await redisConn.TryRemoveAndReleaseLockAsync(dataFromRedis.LockId);

                // Get actual connection and get data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.DataKey));
                
                // check lock removed from redis
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryRemoveIfLockIdMatch_NullLockId()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                await redisConn.SetAsync(data, 900);

                GetItemData dataFromRedis = await redisConn.TryCheckWriteLockAndGetDataAsync();
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Null(dataFromRedis.LockId);
                Assert.Equal(1, dataFromRedis.SessionData.Count);

                await redisConn.TryRemoveAndReleaseLockAsync(null);

                // Get actual connection and get data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.DataKey));

                // check lock removed from redis
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatch_WithValidUpdateAndDelete()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key1"] = "value1";
                data["key2"] = "value2";
                data["key3"] = "value3";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(3, dataFromRedis.SessionData.Count);
                Assert.Equal("value1", dataFromRedis.SessionData["key1"]);
                Assert.Equal("value2", dataFromRedis.SessionData["key2"]);
                Assert.Equal("value3", dataFromRedis.SessionData["key3"]);

                dataFromRedis.SessionData["key2"] = "value2-updated";
                dataFromRedis.SessionData.Remove("key3");
                await redisConn.TryUpdateAndReleaseLockAsync(dataFromRedis.LockId, dataFromRedis.SessionData, 900);

                // Get actual connection and get data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);
                Assert.Equal(2, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection sessionDataFromRedisAsCollection = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    sessionDataFromRedisAsCollection[entry.Name] = RedisUtility.GetObjectFromBytes(entry.Value).ToString();
                }
                Assert.Equal("value1", sessionDataFromRedisAsCollection["key1"]);
                Assert.Equal("value2-updated", sessionDataFromRedisAsCollection["key2"]);

                // check lock removed and remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatch_WithOnlyUpdateAndNoDelete()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key1"] = "value1";
                data["key2"] = "value2";
                data["key3"] = "value3";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(3, dataFromRedis.SessionData.Count);
                Assert.Equal("value1", dataFromRedis.SessionData["key1"]);
                Assert.Equal("value2", dataFromRedis.SessionData["key2"]);
                Assert.Equal("value3", dataFromRedis.SessionData["key3"]);

                dataFromRedis.SessionData["key2"] = "value2-updated";
                await redisConn.TryUpdateAndReleaseLockAsync(dataFromRedis.LockId, dataFromRedis.SessionData, 900);

                // Get actual connection and get data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);
                Assert.Equal(3, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection sessionDataFromRedisAsCollection = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    sessionDataFromRedisAsCollection[entry.Name] = RedisUtility.GetObjectFromBytes(entry.Value).ToString();
                }
                Assert.Equal("value1", sessionDataFromRedisAsCollection["key1"]);
                Assert.Equal("value2-updated", sessionDataFromRedisAsCollection["key2"]);
                Assert.Equal("value3", sessionDataFromRedisAsCollection["key3"]);

                // check lock removed and remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatch_WithNoUpdateAndOnlyDelete()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key1"] = "value1";
                data["key2"] = "value2";
                data["key3"] = "value3";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 900);
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Equal(lockTime.Ticks.ToString(), dataFromRedis.LockId.ToString());
                Assert.Equal(3, dataFromRedis.SessionData.Count);
                Assert.Equal("value1", dataFromRedis.SessionData["key1"]);
                Assert.Equal("value2", dataFromRedis.SessionData["key2"]);
                Assert.Equal("value3", dataFromRedis.SessionData["key3"]);

                dataFromRedis.SessionData.Remove("key3");
                await redisConn.TryUpdateAndReleaseLockAsync(dataFromRedis.LockId, dataFromRedis.SessionData, 900);

                // Get actual connection and get data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);
                Assert.Equal(2, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection sessionDataFromRedisAsCollection = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    sessionDataFromRedisAsCollection[entry.Name] = RedisUtility.GetObjectFromBytes(entry.Value).ToString();
                }
                Assert.Equal("value1", sessionDataFromRedisAsCollection["key1"]);
                Assert.Equal("value2", sessionDataFromRedisAsCollection["key2"]);

                // check lock removed and remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatch_ExpiryTime_OnValidData()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();
            
                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key"] = "value";
                data["key1"] = "value1";
                await redisConn.SetAsync(data, 900);

                // Check that data shoud exists
                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 90);
                Assert.Equal(2, dataFromRedis.SessionData.Count);

                // Update expiry time to only 1 sec and than verify that.
                await redisConn.TryUpdateAndReleaseLockAsync(dataFromRedis.LockId, dataFromRedis.SessionData, 1);
                
                // Wait for 1.1 seconds so that data will expire
                System.Threading.Thread.Sleep(1100);

                // Get data blob from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedisAfterExpire = actualConnection.HashGetAll(redisConn.Keys.DataKey);

                // Check that data shoud not be there
                Assert.Equal(0, sessionDataFromRedisAfterExpire.Length);
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryUpdateAndReleaseLockIfLockIdMatch_LargeLockTime_ExpireManuallyTest()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key1"] = "value1";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, 120000);
                Assert.True(dataFromRedis.IsLockTaken);
                await redisConn.TryUpdateAndReleaseLockAsync(dataFromRedis.LockId, dataFromRedis.SessionData, 900);

                // Get actual connection and check that lock is released
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                actualConnection.KeyDelete(redisConn.Keys.DataKey); 
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatch_LockIdNull()
        {
            ProviderConfiguration pc = Utility.GetDefaultConfigUtility();
            using (RedisServer redisServer = new RedisServer())
            {
                RedisConnectionWrapper redisConn = GetRedisConnectionWrapperWithUniqueSession();

                // Inserting data into redis server
                ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                data["key1"] = "value1";
                await redisConn.SetAsync(data, 900);

                DateTime lockTime = DateTime.Now;
                GetItemData dataFromRedis = await redisConn.TryCheckWriteLockAndGetDataAsync();
                Assert.True(dataFromRedis.IsLockTaken);
                Assert.Null(dataFromRedis.LockId);
                Assert.Equal(1, dataFromRedis.SessionData.Count);
                Assert.Equal("value1", dataFromRedis.SessionData["key1"]);
                
                // update session data without lock id (to support lock free session)
                dataFromRedis.SessionData["key1"] = "value1-updated";
                await redisConn.TryUpdateAndReleaseLockAsync(null, dataFromRedis.SessionData, 900);

                // Get actual connection and get data from redis
                IDatabase actualConnection = GetRealRedisConnection(redisConn);
                HashEntry[] sessionDataFromRedis = actualConnection.HashGetAll(redisConn.Keys.DataKey);
                Assert.Equal(1, sessionDataFromRedis.Length);
                ChangeTrackingSessionStateItemCollection sessionDataFromRedisAsCollection = new ChangeTrackingSessionStateItemCollection(new RedisUtility(pc));
                foreach (HashEntry entry in sessionDataFromRedis)
                {
                    sessionDataFromRedisAsCollection[entry.Name] = RedisUtility.GetObjectFromBytes(entry.Value).ToString();
                }
                Assert.Equal("value1-updated", sessionDataFromRedisAsCollection["key1"]);
                
                // check lock removed and remove data from redis
                actualConnection.KeyDelete(redisConn.Keys.DataKey);
                Assert.False(actualConnection.KeyExists(redisConn.Keys.LockKey));
                await DisposeRedisConnectionWrapper(redisConn);
            }
        }

        private StackExchangeClientConnection GetRedisConnection()
        {
            StackExchangeClientConnection client = new StackExchangeClientConnection(Utility.GetDefaultConfigUtility());
            return client;
        }

        private IDatabase GetRealRedisConnection(RedisConnectionWrapper redisConn)
        {
            return (IDatabase)((StackExchangeClientConnection)redisConn.redisConnection).RealConnection;
        }
    }
}
