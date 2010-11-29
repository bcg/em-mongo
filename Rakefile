
require 'rake'
require 'rake/gempackagetask'

require 'spec/rake/spectask'
require 'fileutils'
require 'tmpdir'

task :default => :spec

class MongoRunner
  def self.run 
    dir = Dir.tmpdir 
    FileUtils.rm_r Dir.glob("#{dir}/*")
    pidf = "#{dir}/mongod.pid"
    logf = "#{dir}/mongo.log"
    begin
      system "mongod run --fork -vvvvvvv --dbpath #{dir} --pidfilepath #{pidf} --logpath #{logf} >> /dev/null "
      yield if block_given?
    ensure
      Process.kill("KILL", File.read(pidf).to_i)
      FileUtils.rm_r Dir.glob("#{dir}/*")
    end
  end
end

spec = eval(File.read('em-mongo.gemspec'))

Rake::GemPackageTask.new(spec) do |pkg|
  pkg.gem_spec = spec
end

desc "rspec tests"
task :spec do
  MongoRunner.run do
    system "bundle exec spec #{spec.test_files.join(' ')} -t -b -fs -color"
  end
end
