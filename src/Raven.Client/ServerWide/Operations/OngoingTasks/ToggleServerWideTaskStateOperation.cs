﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    /// <summary>
    /// Operation for modifying the ongoing task on the server.
    /// </summary>
    public sealed class ToggleServerWideTaskStateOperation : IServerOperation
    {
        private readonly string _name;
        private readonly OngoingTaskType _type;
        private readonly bool _disable;

        /// <inheritdoc cref="ToggleServerWideTaskStateOperation"/>
        /// <param name="name">The name of the ongoing task.</param>
        /// <param name="type">The type of the ongoing type.</param>
        /// <param name="disable">Expected state of the ongoing state (disabled = true/enabled = false)</param>
        /// <exception cref="ArgumentNullException">Thrown when the name is null.</exception>
        public ToggleServerWideTaskStateOperation(string name, OngoingTaskType type, bool disable)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _type = type;
            _disable = disable;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ToggleServerWideTaskStateCommand(_name, _type, _disable);
        }

        private sealed class ToggleServerWideTaskStateCommand : RavenCommand, IRaftCommand
        {
            private readonly string _name;
            private readonly OngoingTaskType _type;
            private readonly bool _disable;

            public ToggleServerWideTaskStateCommand(string name, OngoingTaskType type, bool disable)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _type = type;
                _disable = disable;
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/state?type={_type}&name={Uri.EscapeDataString(_name)}&disable={_disable}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
