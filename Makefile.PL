use ExtUtils::MakeMaker;
WriteMakefile(
    'NAME'      => 'DBIx::Class::Schema::Loader::DBI::Oracle',
    'PREREQ_PM' => {
    Test::More => 0.32,
    DBIx::Class::Schema::Loader => 0.03000,
    DBD::Oracle => 1.16,
    },
);
