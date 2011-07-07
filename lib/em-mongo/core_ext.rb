#:nodoc:
class String

  #:nodoc:
  def to_bson_code
    BSON::Code.new(self)
  end

end