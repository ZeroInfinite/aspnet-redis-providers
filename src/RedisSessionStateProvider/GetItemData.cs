//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
//

using System;
using System.Web.SessionState;

namespace Microsoft.Web.Redis
{
    internal class GetItemData
    {
        public bool IsLockTaken { get; private set; }
        public object LockId { get; private set; }
        public ISessionStateItemCollection SessionData { get; private set; }
        public int SessionTimeout { get; private set; }

        public GetItemData(bool isLockTaken, object lockId, ISessionStateItemCollection sessionData, int sessionTimeout)
        {
            IsLockTaken = isLockTaken;
            LockId = lockId;
            SessionData = sessionData;
            SessionTimeout = sessionTimeout;
        }
    }
}
