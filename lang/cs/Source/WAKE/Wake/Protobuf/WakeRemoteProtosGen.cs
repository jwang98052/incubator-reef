//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// Generated from: src/main/proto/RemoteProtocol.proto
namespace Org.Apache.Reef.Wake.Remote.Proto.WakeRemoteProtos
{
  [global::System.Serializable, global::ProtoBuf.ProtoContract(Name=@"WakeMessagePBuf")]
  public partial class WakeMessagePBuf : global::ProtoBuf.IExtensible
  {
    public WakeMessagePBuf() {}
    
    private byte[] _data;
    [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name=@"data", DataFormat = global::ProtoBuf.DataFormat.Default)]
    public byte[] data
    {
      get { return _data; }
      set { _data = value; }
    }
    private long _seq;
    [global::ProtoBuf.ProtoMember(2, IsRequired = true, Name=@"seq", DataFormat = global::ProtoBuf.DataFormat.TwosComplement)]
    public long seq
    {
      get { return _seq; }
      set { _seq = value; }
    }
    private string _source = "";
    [global::ProtoBuf.ProtoMember(3, IsRequired = false, Name=@"source", DataFormat = global::ProtoBuf.DataFormat.Default)]
    [global::System.ComponentModel.DefaultValue("")]
    public string source
    {
      get { return _source; }
      set { _source = value; }
    }
    private string _sink = "";
    [global::ProtoBuf.ProtoMember(4, IsRequired = false, Name=@"sink", DataFormat = global::ProtoBuf.DataFormat.Default)]
    [global::System.ComponentModel.DefaultValue("")]
    public string sink
    {
      get { return _sink; }
      set { _sink = value; }
    }
    private global::ProtoBuf.IExtension extensionObject;
    global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
      { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
  }
  
  [global::System.Serializable, global::ProtoBuf.ProtoContract(Name=@"WakeTuplePBuf")]
  public partial class WakeTuplePBuf : global::ProtoBuf.IExtensible
  {
    public WakeTuplePBuf() {}
    
    private string _className;
    [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name=@"className", DataFormat = global::ProtoBuf.DataFormat.Default)]
    public string className
    {
      get { return _className; }
      set { _className = value; }
    }
    private byte[] _data;
    [global::ProtoBuf.ProtoMember(2, IsRequired = true, Name=@"data", DataFormat = global::ProtoBuf.DataFormat.Default)]
    public byte[] data
    {
      get { return _data; }
      set { _data = value; }
    }
    private global::ProtoBuf.IExtension extensionObject;
    global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
      { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
  }
  
}