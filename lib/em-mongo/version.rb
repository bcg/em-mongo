module EventMachine
  module Mongo

    VERSION = File.read(File.expand_path('../../../VERSION',__FILE__)).strip

    module Version
      STRING = EventMachine::Mongo::VERSION
      MAJOR, MINOR, TINY = STRING.split('.')
    end
  end
end
