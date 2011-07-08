module EM
  module Mongo
    class RequestResponse
      include EM::Deferrable

      def status
        @deferred_status
      end

      def completed?
        [:succeeded, :failed].include?(status)
      end

      def succeeded?
        status == :succeeded
      end

      def failed?
        status == :failed
      end

      def data
        @deferred_args.pop if succeeded? && @deferred_args
      end

      def error
        @deferred_args.pop if failed? && @deferred_args
      end

    end
  end
end

          