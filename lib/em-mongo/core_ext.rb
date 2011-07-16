#:nodoc:
class String

  #:nodoc:
  def to_bson_code
    BSON::Code.new(self)
  end

end

#:nodoc:
class Hash

  #:nodoc:
  def assert_valid_keys(*valid_keys)
    unknown_keys = keys - [valid_keys].flatten
    raise(ArgumentError, "Unknown key(s): #{unknown_keys.join(", ")}") unless unknown_keys.empty?
  end

end