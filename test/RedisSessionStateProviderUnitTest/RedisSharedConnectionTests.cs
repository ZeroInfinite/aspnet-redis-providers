//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Threading.Tasks;
using System.Web.SessionState;
using Xunit;

namespace Microsoft.Web.Redis.Tests
{
    public class RedisSharedConnectionTests
    {
        [Fact]
        public void TryGetConnection_CreateNewConnection()
        {
            Utility.SetConfigUtilityToDefault();
            RedisSharedConnection redisSharedConnection = new RedisSharedConnection(RedisSessionStateProvider.configuration,
                () => new FakeRedisClientConnection());
            Assert.Null(redisSharedConnection.connection);
            IRedisClientConnection connection = redisSharedConnection.TryGetConnection();
            Assert.NotNull(connection);
            Assert.NotNull(redisSharedConnection.connection);
        }

        [Fact]
        public void TryGetConnection_ConnectionSharing()
        {
            Utility.SetConfigUtilityToDefault();
            RedisSharedConnection redisSharedConnection = new RedisSharedConnection(RedisSessionStateProvider.configuration,
                () => new FakeRedisClientConnection());
            IRedisClientConnection connection = redisSharedConnection.TryGetConnection();
            IRedisClientConnection connection2 = redisSharedConnection.TryGetConnection();
            Assert.Equal(connection, connection2);
        }

    }

    class FakeRedisClientConnection : IRedisClientConnection
    {
        public FakeRedisClientConnection()
        { }
        
        public virtual async Task CloseAsync()
        {
            await Task.FromResult(0);
        }

        public virtual async Task<bool> ExpiryAsync(string key, int timeInSeconds)
        {
            return await Task.FromResult(false);
        }

        public virtual async Task<object> EvalAsync(string script, string[] keyArgs, object[] valueArgs)
        {
            return await Task.FromResult((object)null);
        }
        
        public string GetLockId(object rowDataFromRedis)
        {
            return null;
        }

        public bool IsLocked(object rowDataFromRedis)
        {
            return false;
        }

        public int GetSessionTimeout(object rowDataFromRedis)
        {
            return 1200;
        }

        public ISessionStateItemCollection GetSessionData(object rowDataFromRedis)
        {
            return null;
        }

        public virtual async Task SetAsync(string key, byte[] data, DateTime utcExpiry)
        {
            await Task.FromResult(0);
        }

        public virtual async Task<byte[]> GetAsync(string key)
        {
            return await Task.FromResult((byte[])null);
        }

        public virtual async Task RemoveAsync(string key)
        {
            await Task.FromResult(0);
        }

        public byte[] GetOutputCacheDataFromResult(object rowDataFromRedis)
        {
            return null;
        }
    }
}
