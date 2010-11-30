
require 'rake'
require 'rake/gempackagetask'

require 'spec/rake/spectask'
require 'fileutils'
require 'tmpdir'

task :default => 'spec:integration:default'

class MongoRunner
  def self.run(options={}) 
    auth = "--auth" if options[:auth]
    dir = Dir.tmpdir 
    FileUtils.rm_r Dir.glob("#{dir}/*") unless options[:noclean]
    pidf = "#{dir}/mongod.pid"
    logf = "#{dir}/mongo.log"
    begin
      #puts "mongod run #{auth} --fork -vvvvvvv --dbpath #{dir} --pidfilepath #{pidf} --logpath #{logf} >> /dev/null "
      system "mongod run #{auth} --fork -vvvvvvv --dbpath #{dir} --pidfilepath #{pidf} --logpath #{logf} >> /dev/null "
      yield if block_given?
    ensure
      Process.kill("KILL", File.read(pidf).to_i)
      FileUtils.rm_r Dir.glob("#{dir}/*") unless options[:noclean]
    end
  end
end

spec = eval(File.read('em-mongo.gemspec'))

Rake::GemPackageTask.new(spec) do |pkg|
  pkg.gem_spec = spec
end

namespace :spec do
  namespace :integration do
    desc "default tests"
    task :default do
      MongoRunner.run do
        system "bundle exec spec #{spec.test_files.join(' ')} -t -b -fs -color"
      end
    end

    desc "exhaustive tests"
    task :exhaustive do
      MongoRunner.run({:noclean => true}) do
        system "bundle exec spec #{spec.test_files.join(' ')} -t -b -fs -color"
      end
      MongoRunner.run({:auth => true}) do
        system "bundle exec spec #{spec.test_files.join(' ')} -t -b -fs -color"
      end
    end

    desc "provide your own mongo instance"
    task :no_mongo do
      system "bundle exec spec #{spec.test_files.join(' ')} -t -b -fs -color"
    end
  end
end
