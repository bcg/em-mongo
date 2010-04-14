
require 'rake'

require 'spec/rake/spectask'
require 'fileutils'

class MongoRunner
  
  def self.mongo_db_dir
    "/tmp/mongo_db/"
  end
  
  def self.dtach_socket
    '/tmp/mongo.dtach'
  end

  def self.init
    FileUtils.mkdir(mongo_db_dir) if not File.exists?(mongo_db_dir)
    FileUtils.rm_r Dir.glob("#{mongo_db_dir}/*")
    self.stop if File.exists?("#{mongo_db_dir}/mongod.lock")
  end
 
  def self.running?
    File.exists? dtach_socket
  end
  
  def self.start
    self.init
    puts 'Detach with Ctrl+\  Re-attach with rake mongodb:attach'
    sleep 2
    exec "dtach -A #{dtach_socket} mongod run -vvvvvvv --objcheck --dbpath #{mongo_db_dir}"
  end
  
  def self.start_detached
    self.init
    system "dtach -n #{dtach_socket} mongod run -vvvvvvvv --objcheck --logpath #{mongo_db_dir}/../mongodb.log --dbpath #{mongo_db_dir}"
  end
  
  def self.attach
    exec "dtach -a #{dtach_socket}"
  end
  
  def self.stop
    system "cat #{mongo_db_dir}/mongod.lock | xargs kill"
  end
 
end


namespace :mongodb do
  desc "start mongodb"
  task :start do
    MongoRunner.start
  end

  desc "start mongodb in the background"
  task :start_detached do
    MongoRunner.start_detached
  end

  desc "stop mongodb"
  task :stop do
    MongoRunner.stop
  end
end

desc "rspec tests"
task :spec do
  puts "We only support tests against a running MongoDB instance now"
  #exec "bundle exec spec spec/*.rb -b -fs -color"
end

desc "run specs against mongodb"
task :test do
  begin
    Rake::Task["mongodb:start_detached"].invoke
    sleep 1
    Rake::Task["spec"].invoke
  ensure
    Rake::Task["mongodb:stop"].invoke 
  end
end

