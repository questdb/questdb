{
  'conditions': [
    ['OS=="linux"', {
      'variables' : {
        # Find the pull path to the pg_config command, since iy may not be on the PATH
        'pgconfig': '<!(which pg_config || find /usr/bin /usr/local/bin /usr/pg* /opt -executable -name pg_config -print -quit)'
      }
    }, {
      #Default to assuming pg_config is on the PATH.
      'variables': {
        'pgconfig': 'pg_config'
      },
    }]
  ],
  'targets': [
    {
      'target_name': 'addon',
      'sources': [
        'src/connection.cc',
        'src/connect-async-worker.cc',
        'src/addon.cc'
      ],
      'include_dirs': [
        '<!@(<(pgconfig) --includedir)',
        '<!(node -e "require(\'nan\')")'
      ],
      'conditions' : [
        ['OS=="linux"', {
            'cflags': ['-fvisibility=hidden']
        }],
        ['OS=="win"', {
          'libraries' : ['libpq.lib'],
          'msvs_settings': {
            'VCLinkerTool' : {
              'AdditionalLibraryDirectories' : [
                '<!@(<(pgconfig) --libdir)\\'
              ]
            },
          }
        }, { # OS!="win"
          'libraries' : ['-lpq -L<!@(<(pgconfig) --libdir)'],
          'ldflags' : ['<!@(<(pgconfig) --ldflags)']
        }],
        ['OS=="mac"', {
          'xcode_settings': {
            'CLANG_CXX_LIBRARY': 'libc++',
            'MACOSX_DEPLOYMENT_TARGET': '10.7'
          }
        }]
      ]
    }
  ]
}
