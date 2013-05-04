class wrasse {
  exec {
    'update apt':
      refreshonly => true,
      command     => '/usr/bin/apt-get update';
  }

  package {
    'reprepro':
      ensure  => present,
      require => Exec["update apt"];
  }
}

include wrasse
