
require 'fileutils'
require 'tmpdir'

def em_mongo_version
  File.read("VERSION").strip
end

def root_dir
  File.dirname(__FILE__)
end

task :default => 'spec:integration:default'

class MongoRunner
  def self.run(options={}) 
    auth = "--auth" if options[:auth]
    dir = Dir.tmpdir + "/em-mongo-tests-#$$"
    FileUtils.mkdir dir
    pidf = "#{dir}/mongod.pid"
    logf = "#{dir}/mongo.log"
    begin
      system "mongod run #{auth} --fork -vvvvvvv --dbpath #{dir} --pidfilepath #{pidf} --logpath #{logf} >> /dev/null "
      yield if block_given?
    ensure
      if File.exists?(pidf) and File.read(pidf).to_i != 0
        Process.kill("KILL", File.read(pidf).to_i)
        FileUtils.rm_r dir unless options[:noclean]
      end
    end
  end
end

spec = eval(File.read('em-mongo.gemspec'))

namespace :bundle do
  task :install do
    if `bundle check` =~ /bundle install/
      system("bundle install --path vendor/gems")
    end
  end
end

namespace :gem do

  desc "build gem"
  task :build do
    puts "Building em-mongo #{em_mongo_version}"
    system "gem build em-mongo.gemspec -q"
  end

  desc "release gem"
  task :release do
    system "gem push em-mongo-#{em_mongo_version}.gem"
  end

end

namespace :spec do
  
  namespace :gem do

    desc "bundler tests"
    task :bundler do
      MongoRunner.run do
        print "Testing Bundler integration ... "
        if system "cd spec/gem && bundle install --quiet && ./bundler.rb"
          puts "SUCCESS."
        else
          puts "FAILURE."
        end
      end
    end

    desc "rubygems tests"
    task :rubygems do
      MongoRunner.run do
        print "Testing Rubygems integration ... "
        steps =[]
        steps << "cd spec/gem"
        
        if `gem list -i em-mongo` == 'true'
          steps << "gem uninstall --force -a em-mongo >/dev/null"
        end
        steps << "gem install #{root_dir}/em-mongo-#{em_mongo_version}.gem >/dev/null "
        steps << "./rubygems.rb"
        if system steps.join(" && ")
          puts "SUCCESS."
        else
          puts "FAILURE."
        end
      end
    end

    desc "all gem tests"
    task :all => [:bundler, :rubygems]
  end

  namespace :integration do

    desc "default tests"
    task :default => ['bundle:install'] do
      MongoRunner.run do
        files = spec.test_files.select{|x| x =~ /integration/}.join(' ')
        system "bundle exec spec #{files} -t -b -fs -color"
      end
    end

    desc "exhaustive tests"
    task :exhaustive => ['bundle:install'] do
      MongoRunner.run({:noclean => true}) do
        files = spec.test_files.select{|x| x =~ /integration/}.join(' ')
        system "bundle exec spec #{files} -t -b -fs -color"
      end
      MongoRunner.run({:auth => true}) do
        files = spec.test_files.select{|x| x =~ /integration/}.join(' ')
        system "bundle exec spec #{files} -t -b -fs -color"
      end
    end

    desc "default tests, but don't start mongodb for me"
    task :no_mongo => ['bundle:install'] do
      files = spec.test_files.select{|x| x =~ /integration/}.join(' ')
      system "bundle exec spec #{files} -t -b -fs -color"
    end

  end

  desc "release testing"
  task :release => ['spec:integration:default','gem:build','spec:gem:bundler','spec:gem:rubygems']
end
