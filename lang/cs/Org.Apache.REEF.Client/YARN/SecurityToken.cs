//<auto-generated />
namespace org.apache.reef.bridge.client
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using Microsoft.Hadoop.Avro;

    /// <summary>
    /// Used to serialize and deserialize Avro record org.apache.reef.bridge.client.SecurityToken.
    /// </summary>
    [DataContract(Namespace = "org.apache.reef.bridge.client")]
    public partial class SecurityToken
    {
        private const string JsonSchema = @"{""type"":""record"",""name"":""org.apache.reef.bridge.client.SecurityToken"",""doc"":""Security token"",""fields"":[{""name"":""kind"",""doc"":""Kind of the security token"",""type"":""string""},{""name"":""service"",""doc"":""Token service name"",""type"":""string""},{""name"":""key"",""doc"":""Token key"",""type"":""bytes""},{""name"":""password"",""doc"":""Token password"",""type"":""bytes""}]}";

        /// <summary>
        /// Gets the schema.
        /// </summary>
        public static string Schema
        {
            get
            {
                return JsonSchema;
            }
        }
      
        /// <summary>
        /// Gets or sets the kind field.
        /// </summary>
        [DataMember]
        public string kind { get; set; }
              
        /// <summary>
        /// Gets or sets the service field.
        /// </summary>
        [DataMember]
        public string service { get; set; }
              
        /// <summary>
        /// Gets or sets the key field.
        /// </summary>
        [DataMember]
        public byte[] key { get; set; }
              
        /// <summary>
        /// Gets or sets the password field.
        /// </summary>
        [DataMember]
        public byte[] password { get; set; }
                
        /// <summary>
        /// Initializes a new instance of the <see cref="SecurityToken"/> class.
        /// </summary>
        public SecurityToken()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SecurityToken"/> class.
        /// </summary>
        /// <param name="kind">The kind.</param>
        /// <param name="service">The service.</param>
        /// <param name="key">The key.</param>
        /// <param name="password">The password.</param>
        public SecurityToken(string kind, string service, byte[] key, byte[] password)
        {
            this.kind = kind;
            this.service = service;
            this.key = key;
            this.password = password;
        }
    }
}
