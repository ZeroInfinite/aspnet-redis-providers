//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using Xunit;
using FakeItEasy;
using System.Collections.Specialized;
using System.Configuration.Provider;
using System.Web.SessionState;
using System.Collections.Generic;
using System.Web.Configuration;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.AspNet.SessionState;

namespace Microsoft.Web.Redis.Tests
{
    public class RedisSessionStateProviderTests
    {
        [Fact]
        public void Initialize_WithNullConfig()
        {
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            Assert.Throws<ArgumentNullException>(() => sessionStateStore.Initialize(null, null));
        }

        [Fact]
        public async Task EndRequest_Successful()
        {
            Utility.SetConfigUtilityToDefault();
            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.sessionId = "session-id";
            sessionStateStore.sessionLockId = "session-lock-id";
            sessionStateStore.cache = mockCache;
            await sessionStateStore.EndRequestAsync(null);
            A.CallTo(() => mockCache.TryReleaseLockIfLockIdMatchAsync(A<object>.Ignored, A<int>.Ignored)).MustHaveHappened();
        }

        [Fact]
        public void CreateNewStoreData_WithEmptyStore()
        {
            Utility.SetConfigUtilityToDefault();
            SessionStateStoreData sssd = new SessionStateStoreData(Utility.GetChangeTrackingSessionStateItemCollection(), null, 900);
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            Assert.Equal(true, Utility.CompareSessionStateStoreData(sessionStateStore.CreateNewStoreData(null, 900),sssd));
        }

        [Fact]
        public async Task CreateUninitializedItem_Successful()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id"; 
            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.CreateUninitializedItemAsync(null, id, 15, CancellationToken.None);
            A.CallTo(() => mockCache.SetAsync(A<ISessionStateItemCollection>.That.Matches(
                o => o.Count == 1 && SessionStateActions.InitializeItem.Equals(o["SessionStateActions"]) 
                ), 900)).MustHaveHappened();
        }

        [Fact]
        public async Task GetItem_NullFromStore()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";

            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = A.Fake<ICacheConnection>();

            var callToTryCheckWriteLockAndGetDataAsync = A.CallTo(() => sessionStateStore.cache.TryCheckWriteLockAndGetDataAsync());
            callToTryCheckWriteLockAndGetDataAsync.Returns(Task.FromResult(new GetItemData(true, 0, null, 0))); 
            
            GetItemResult data = await sessionStateStore.GetItemAsync(null, id, CancellationToken.None);

            callToTryCheckWriteLockAndGetDataAsync.MustHaveHappened();
            A.CallTo(() => sessionStateStore.cache.TryReleaseLockIfLockIdMatchAsync(data.LockId, A<int>.Ignored)).MustHaveHappened(); 
            
            Assert.Equal(null, data.Item);
            Assert.Equal(false, data.Locked);
            Assert.Equal(TimeSpan.Zero, data.LockAge);
            Assert.Equal(0, data.LockId);
        }

        [Fact]
        public async Task GetItem_RecordLocked()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";

            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = A.Fake<ICacheConnection>();

            var callToTryCheckWriteLockAndGetDataAsync = A.CallTo(() => sessionStateStore.cache.TryCheckWriteLockAndGetDataAsync());
            callToTryCheckWriteLockAndGetDataAsync.Returns(Task.FromResult(new GetItemData(false, null, null, 0)));
            var callToGetLockAge = A.CallTo(() => sessionStateStore.cache.GetLockAge(A<object>.Ignored));
            callToGetLockAge.Returns(TimeSpan.Zero);

            GetItemResult data = await sessionStateStore.GetItemAsync(null, id, CancellationToken.None);

            callToTryCheckWriteLockAndGetDataAsync.MustHaveHappened();
            callToGetLockAge.MustHaveHappened();
            
            Assert.Equal(null, data.Item);
            Assert.Equal(true, data.Locked);
        }

        [Fact]
        public async Task GetItem_RecordFound()
        {
            Utility.SetConfigUtilityToDefault();
            string id = "session-id";
            
            ISessionStateItemCollection sessionStateItemCollection = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionStateItemCollection["session-key"] = "session-value";
            sessionStateItemCollection["SessionStateActions"] = SessionStateActions.None;
            SessionStateStoreData sssd = new SessionStateStoreData(sessionStateItemCollection, null, 15);

            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = A.Fake<ICacheConnection>();

            var callToTryCheckWriteLockAndGetDataAsync = A.CallTo(() => sessionStateStore.cache.TryCheckWriteLockAndGetDataAsync());
            callToTryCheckWriteLockAndGetDataAsync.Returns(
                Task.FromResult(new GetItemData(true, 0, sessionStateItemCollection, (int)RedisSessionStateProvider.configuration.SessionTimeout.TotalMinutes)));

            GetItemResult data = await sessionStateStore.GetItemAsync(null, id, CancellationToken.None);
            
            callToTryCheckWriteLockAndGetDataAsync.MustHaveHappened();
            Assert.Equal(true, Utility.CompareSessionStateStoreData(data.Item, sssd));
            Assert.Equal(false, data.Locked);
            Assert.Equal(TimeSpan.Zero, data.LockAge);
            Assert.Equal(SessionStateActions.None, data.Actions);
        }

        [Fact]
        public async Task GetItemExclusive_RecordLocked()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";
            
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = A.Fake<ICacheConnection>();

            var callToTryTakeWriteLockAndGetDataAsync = A.CallTo(() => sessionStateStore.cache.TryTakeWriteLockAndGetDataAsync(A<DateTime>.Ignored, 90));
            callToTryTakeWriteLockAndGetDataAsync.Returns(Task.FromResult(new GetItemData(false, null, null, 0)));
            var callToGetLockAge = A.CallTo(() => sessionStateStore.cache.GetLockAge(A<object>.Ignored));
            callToGetLockAge.Returns(TimeSpan.Zero);
            
            GetItemResult data = await sessionStateStore.GetItemExclusiveAsync(null, id, CancellationToken.None);

            callToTryTakeWriteLockAndGetDataAsync.MustHaveHappened();
            callToGetLockAge.MustHaveHappened();

            Assert.Equal(null, data.Item);
            Assert.Equal(true, data.Locked);
        }

        [Fact]
        public async Task GetItemExclusive_RecordFound()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";
            
            ISessionStateItemCollection sessionStateItemCollection = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionStateItemCollection["session-key"] = "session-value";
            SessionStateStoreData sssd = new SessionStateStoreData(sessionStateItemCollection, null, 15);

            var mockCache = A.Fake<ICacheConnection>();
            var callToTryTakeWriteLockAndGetDataAsync = A.CallTo(() => mockCache.TryTakeWriteLockAndGetDataAsync(A<DateTime>.Ignored, 90));
            callToTryTakeWriteLockAndGetDataAsync.Returns(Task.FromResult(new GetItemData(true, 0, sessionStateItemCollection, (int)RedisSessionStateProvider.configuration.SessionTimeout.TotalMinutes)));
            
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            GetItemResult data = await sessionStateStore.GetItemExclusiveAsync(null, id, CancellationToken.None);

            callToTryTakeWriteLockAndGetDataAsync.MustHaveHappened();
            Assert.Equal(true, Utility.CompareSessionStateStoreData(data.Item, sssd));
            Assert.Equal(false, data.Locked);
            Assert.Equal(TimeSpan.Zero, data.LockAge);
            Assert.Equal(data.Actions, SessionStateActions.None);
        }

        [Fact]
        public async Task ResetItemTimeout_Successful()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";
            var mockCache = A.Fake<ICacheConnection>();
            
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.ResetItemTimeoutAsync(null, id, CancellationToken.None);
            A.CallTo(() => mockCache.UpdateExpiryTimeAsync(900)).MustHaveHappened();
        }

        [Fact]
        public async Task RemoveItem_Successful()
        {
            Utility.SetConfigUtilityToDefault();
            string id = "session-id";
            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.RemoveItemAsync(null, id, "lockId", null, CancellationToken.None);
            A.CallTo(() => mockCache.TryRemoveAndReleaseLockAsync(A<object>.Ignored)).MustHaveHappened();
        }

        [Fact]
        public async Task ReleaseItemExclusive_Successful()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";
            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.ReleaseItemExclusiveAsync(null, id, "lockId", CancellationToken.None);
            A.CallTo(() => mockCache.TryReleaseLockIfLockIdMatchAsync(A<object>.Ignored, A<int>.Ignored)).MustHaveHappened();
        }

        [Fact]
        public async Task SetAndReleaseItemExclusive_NewItemNullItems()
        {
            Utility.SetConfigUtilityToDefault(); 
            string id = "session-id";
            SessionStateStoreData sssd = new SessionStateStoreData(null, null, 15);

            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.SetAndReleaseItemExclusiveAsync(null, id, sssd, null, true, CancellationToken.None);
            A.CallTo(() => mockCache.SetAsync(A<ISessionStateItemCollection>.That.Matches(o => o.Count == 0), 900)).MustHaveHappened();
        }

        [Fact]
        public async Task SetAndReleaseItemExclusive_NewItemValidItems()
        {
            Utility.SetConfigUtilityToDefault();
            string id = "session-id";
            ChangeTrackingSessionStateItemCollection sessionStateItemCollection = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionStateItemCollection["session-key"] = "session-value";
            SessionStateStoreData sssd = new SessionStateStoreData(sessionStateItemCollection, null, 15);

            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.SetAndReleaseItemExclusiveAsync(null, id, sssd, null, true, CancellationToken.None);
            A.CallTo(() => mockCache.SetAsync(A<ISessionStateItemCollection>.That.Matches(
                o => o.Count == 1 && o["session-key"] != null
                ), 900)).MustHaveHappened();
        }

        [Fact]
        public async Task SetAndReleaseItemExclusive_OldItemNullItems()
        {
            Utility.SetConfigUtilityToDefault();
            string id = "session-id";
            SessionStateStoreData sssd = new SessionStateStoreData(null, null, 900);

            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.SetAndReleaseItemExclusiveAsync(null, id, sssd, 7, false, CancellationToken.None);
            A.CallTo(() => mockCache.TryUpdateAndReleaseLockAsync(A<object>.Ignored, A<ISessionStateItemCollection>.Ignored, 900)).MustNotHaveHappened();
        }

        [Fact]
        public async Task SetAndReleaseItemExclusive_OldItemRemovedItems()
        {
            Utility.SetConfigUtilityToDefault();
            string id = "session-id";
            ChangeTrackingSessionStateItemCollection sessionStateItemCollection = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionStateItemCollection["session-key"] = "session-val";
            sessionStateItemCollection.Remove("session-key");
            SessionStateStoreData sssd = new SessionStateStoreData(sessionStateItemCollection, null, 15);

            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.SetAndReleaseItemExclusiveAsync(null, id, sssd, 7, false, CancellationToken.None);
            A.CallTo(() => mockCache.TryUpdateAndReleaseLockAsync(A<object>.Ignored, 
                A<ChangeTrackingSessionStateItemCollection>.That.Matches(o => o.Count == 0 && o.GetModifiedKeys().Count == 0 && o.GetDeletedKeys().Count == 1), 900)).MustHaveHappened();
        }

        [Fact]
        public async Task SetAndReleaseItemExclusive_OldItemInsertedItems()
        {
            Utility.SetConfigUtilityToDefault();
            string id = "session-id";
            ChangeTrackingSessionStateItemCollection sessionStateItemCollection = Utility.GetChangeTrackingSessionStateItemCollection();
            sessionStateItemCollection["session-key"] = "session-value";
            SessionStateStoreData sssd = new SessionStateStoreData(sessionStateItemCollection, null, 15);

            var mockCache = A.Fake<ICacheConnection>();
            RedisSessionStateProvider sessionStateStore = new RedisSessionStateProvider();
            sessionStateStore.cache = mockCache;
            await sessionStateStore.SetAndReleaseItemExclusiveAsync(null, id, sssd, 7, false, CancellationToken.None);
            A.CallTo(() => mockCache.TryUpdateAndReleaseLockAsync(A<object>.Ignored, 
                A<ChangeTrackingSessionStateItemCollection>.That.Matches(o => o.Count == 1 && o.GetModifiedKeys().Count == 1 && o.GetDeletedKeys().Count == 0), 900)).MustHaveHappened();  
        }
    }
}
