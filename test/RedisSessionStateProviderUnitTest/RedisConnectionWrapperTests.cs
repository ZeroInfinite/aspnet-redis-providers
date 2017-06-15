//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using FakeItEasy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.SessionState;
using Xunit;

namespace Microsoft.Web.Redis.Tests
{
    public class RedisConnectionWrapperTests
    {
        private static RedisUtility RedisUtility = new RedisUtility(Utility.GetDefaultConfigUtility());

        [Fact]
        public async Task UpdateExpiryTime_Valid()
        {
            string sessionId = "session_id";
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), sessionId);
            await redisConn.UpdateExpiryTimeAsync(90);
            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 2),
                A<object[]>.That.Matches(o => o.Length == 1))).MustHaveHappened();
        }

        [Fact]
        public void GetLockAge_ValidTicks()
        {
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;

            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), "");
            Assert.NotNull(redisConn.GetLockAge(DateTime.Now.Ticks));
        }

        [Fact]
        public void GetLockAge_InValidTicks()
        {
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;

            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), "");
            Assert.NotEqual(0, redisConn.GetLockAge("Invalid-tics").TotalHours);
        }

        [Fact]
        public async Task Set_NullData()
        {
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            
            string sessionId = "session_id";
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), sessionId);
            await redisConn.SetAsync(null, 90);
            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.Ignored, A<object[]>.Ignored)).MustNotHaveHappened();
        }

        [Fact]
        public async Task Set_ValidData()
        {
            string sessionId = "session_id";
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.redisUtility = new RedisUtility(Utility.GetDefaultConfigUtility()); 
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), sessionId);
            ChangeTrackingSessionStateItemCollection data = new ChangeTrackingSessionStateItemCollection(RedisConnectionWrapper.redisUtility);
            data["key"] = "value";
            await redisConn.SetAsync(data, 90);
            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 2), 
                A<object[]>.That.Matches(o => o.Length == 4))).MustHaveHappened();
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_UnableToLock()
        {
            string id = "session_id";
            DateTime lockTime = DateTime.Now;
            int lockTimeout = 90;
            
            object[] returnFromRedis = { "Diff-lock-id", "", "15", true };

            var mockRedisClient = A.Fake<IRedisClientConnection>();
            var callToEvalAsync = A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3), 
                A<object[]>.That.Matches(o => o.Length == 2)));
            callToEvalAsync.Returns(Task.FromResult((object)returnFromRedis));

            var callToGetLockId = A.CallTo(() => mockRedisClient.GetLockId(A<object>.Ignored));
            callToGetLockId.Returns("Diff-lock-id");
            var callToIsLocked = A.CallTo(() => mockRedisClient.IsLocked(A<object>.Ignored));
            callToIsLocked.Returns(true);
            var callToGetSessionTimeout = A.CallTo(() => mockRedisClient.GetSessionTimeout(A<object>.Ignored));
            callToGetSessionTimeout.Returns(15);

            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);

            var data = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, lockTimeout);
            Assert.False(data.IsLockTaken);
            Assert.Equal("Diff-lock-id", data.LockId);
            Assert.Null(data.SessionData);
            Assert.Equal(15, data.SessionTimeout);

            callToEvalAsync.MustHaveHappened();
            callToGetLockId.MustHaveHappened();
            callToIsLocked.MustHaveHappened();
            A.CallTo(() => mockRedisClient.GetSessionData(A<object>.Ignored)).MustNotHaveHappened();
            callToGetSessionTimeout.MustHaveHappened();
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_UnableToLockWithSameLockId()
        {
            string id = "session_id";
            DateTime lockTime = DateTime.Now;
            int lockTimeout = 90;
            
            object[] returnFromRedis = { lockTime.Ticks.ToString(), "", "15", true };

            var mockRedisClient = A.Fake<IRedisClientConnection>();
            var callToEvalAsync = A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3),
                 A<object[]>.That.Matches(o => o.Length == 2)));
            callToEvalAsync.Returns(Task.FromResult((object)returnFromRedis));
            var callToGetLockId = A.CallTo(() => mockRedisClient.GetLockId(A<object>.Ignored));
            callToGetLockId.Returns(lockTime.Ticks.ToString());
            var callToIsLocked = A.CallTo(() => mockRedisClient.IsLocked(A<object>.Ignored));
            callToIsLocked.Returns(true);
            var callToGetSessionTimeout = A.CallTo(() => mockRedisClient.GetSessionTimeout(A<object>.Ignored));
            callToGetSessionTimeout.Returns(15);

            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);

            var data = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, lockTimeout);
            Assert.False(data.IsLockTaken);
            Assert.Equal(lockTime.Ticks.ToString(), data.LockId);
            Assert.Null(data.SessionData);
            Assert.Equal(15, data.SessionTimeout);

            callToEvalAsync.MustHaveHappened();
            callToGetLockId.MustHaveHappened();
            callToIsLocked.MustHaveHappened();
            A.CallTo(() => mockRedisClient.GetSessionData(A<object>.Ignored)).MustNotHaveHappened();
            callToGetSessionTimeout.MustHaveHappened();
        }

        [Fact]
        public async Task TryTakeWriteLockAndGetData_Valid()
        {
            string id = "session_id";
            DateTime lockTime = DateTime.Now;
            int lockTimeout = 90;
            
            object[] sessionData = { "Key", RedisUtility.GetBytesFromObject("value") };
            object[] returnFromRedis = { lockTime.Ticks.ToString(), sessionData, "15", false };
            ChangeTrackingSessionStateItemCollection sessionDataReturn = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionDataReturn["key"] = "value";

            var mockRedisClient = A.Fake<IRedisClientConnection>();
            var callToEvalAsync = A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3),
                 A<object[]>.That.Matches(o => o.Length == 2)));
            callToEvalAsync.Returns(Task.FromResult((object)returnFromRedis));
            var callToGetLockId = A.CallTo(() => mockRedisClient.GetLockId(A<object>.Ignored));
            callToGetLockId.Returns(lockTime.Ticks.ToString());
            var callToIsLocked = A.CallTo(() => mockRedisClient.IsLocked(A<object>.Ignored));
            callToIsLocked.Returns(false);
            var callToGetSessionData = A.CallTo(() => mockRedisClient.GetSessionData(A<object>.Ignored));
            callToGetSessionData.Returns(sessionDataReturn);
            var callToGetSessionTimeout = A.CallTo(() => mockRedisClient.GetSessionTimeout(A<object>.Ignored));
            callToGetSessionTimeout.Returns(15);

            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);

            var data = await redisConn.TryTakeWriteLockAndGetDataAsync(lockTime, lockTimeout);
            Assert.True(data.IsLockTaken);
            Assert.Equal(lockTime.Ticks.ToString(), data.LockId);
            Assert.Equal(1, data.SessionData.Count);
            Assert.Equal(15, data.SessionTimeout);

            callToEvalAsync.MustHaveHappened();
            callToGetLockId.MustHaveHappened();
            callToIsLocked.MustHaveHappened();
            callToGetSessionData.MustHaveHappened();
            callToGetSessionTimeout.MustHaveHappened();
        }

        [Fact]
        public async Task TryCheckWriteLockAndGetData_Valid()
        {
            string id = "session_id";
            object[] sessionData = { "Key", RedisUtility.GetBytesFromObject("value") };
            object[] returnFromRedis = { "", sessionData, "15" };
            ChangeTrackingSessionStateItemCollection sessionDataReturn = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionDataReturn["key"] = "value";

            var mockRedisClient = A.Fake<IRedisClientConnection>();
            var callToEvalAsync = A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3),
                 A<object[]>.That.Matches(o => o.Length == 0)));
            callToEvalAsync.Returns(Task.FromResult((object)returnFromRedis));
            var callToGetLockId = A.CallTo(() => mockRedisClient.GetLockId(A<object>.Ignored));
            callToGetLockId.Returns("");
            var callToGetSessionData = A.CallTo(() => mockRedisClient.GetSessionData(A<object>.Ignored));
            callToGetSessionData.Returns(sessionDataReturn);
            var callToGetSessionTimeout = A.CallTo(() => mockRedisClient.GetSessionTimeout(A<object>.Ignored));
            callToGetSessionTimeout.Returns(15);

            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);

            var data = await redisConn.TryCheckWriteLockAndGetDataAsync();
            Assert.True(data.IsLockTaken);
            Assert.Equal(null, data.LockId);
            Assert.Equal(1, data.SessionData.Count);
            Assert.Equal(15, data.SessionTimeout);

            callToEvalAsync.MustHaveHappened();
            callToGetLockId.MustHaveHappened();
            callToGetSessionData.MustHaveHappened();
            callToGetSessionTimeout.MustHaveHappened();
        }

        [Fact]
        public async Task TryReleaseLockIfLockIdMatch_WriteLock()
        {
            string id = "session_id";
            object lockId = DateTime.Now.Ticks;
            
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);
            
            await redisConn.TryReleaseLockIfLockIdMatchAsync(lockId, 900);
            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3 && s[0].Equals(redisConn.Keys.LockKey)),
                 A<object[]>.That.Matches(o => o.Length == 2))).MustHaveHappened();
        }

        [Fact]
        public async Task TryRemoveIfLockIdMatch_Valid()
        {
            string id = "session_id";
            object lockId = DateTime.Now.Ticks;

            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);
            
            await redisConn.TryRemoveAndReleaseLockIfLockIdMatchAsync(lockId);
            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3),
                 A<object[]>.That.Matches(o => o.Length == 1))).MustHaveHappened();
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatchPrepare_NoUpdateNoDelete()
        {
            string id = "session_id";
            int sessionTimeout = 900;
            object lockId = DateTime.Now.Ticks;
            ChangeTrackingSessionStateItemCollection data = Utility.GetChangeTrackingSessionStateItemCollection();
            
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.redisUtility = new RedisUtility(Utility.GetDefaultConfigUtility());
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);
            await redisConn.TryUpdateAndReleaseLockIfLockIdMatchAsync(lockId, data, sessionTimeout);

            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3), A<object[]>.That.Matches(
               o => o.Length == 8 &&
                    o[2].Equals(0) &&
                    o[3].Equals(9) &&
                    o[4].Equals(8) &&
                    o[5].Equals(0) &&
                    o[6].Equals(9) &&
                    o[7].Equals(8)
                ))).MustHaveHappened();
        }

        [Fact]
        public async Task TryUpdateIfLockIdMatchPrepare_Valid_OneUpdateOneDelete()
        {
            string id = "session_id";
            int sessionTimeout = 900;
            object lockId = DateTime.Now.Ticks;
            ChangeTrackingSessionStateItemCollection data = Utility.GetChangeTrackingSessionStateItemCollection();
            data["KeyDel"] = "valueDel";
            data["Key"] = "value";
            data.Remove("KeyDel");

            
            var mockRedisClient = A.Fake<IRedisClientConnection>();
            RedisConnectionWrapper.redisUtility = new RedisUtility(Utility.GetDefaultConfigUtility());
            RedisConnectionWrapper.sharedConnection = new RedisSharedConnection(null, null);
            RedisConnectionWrapper.sharedConnection.connection = mockRedisClient;
            RedisConnectionWrapper redisConn = new RedisConnectionWrapper(Utility.GetDefaultConfigUtility(), id);
            await redisConn.TryUpdateAndReleaseLockIfLockIdMatchAsync(lockId, data, sessionTimeout);

            A.CallTo(() => mockRedisClient.EvalAsync(A<string>.Ignored, A<string[]>.That.Matches(s => s.Length == 3), A<object[]>.That.Matches(
               o => o.Length == 11 &&
                    o[2].Equals(1) &&
                    o[3].Equals(9) &&
                    o[4].Equals(9) &&
                    o[5].Equals(1) &&
                    o[6].Equals(10) &&
                    o[7].Equals(11)
                ))).MustHaveHappened();
        }

    }
}
