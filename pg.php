<?php
define( 'PG_NO_BOUND', "\x00no-param\x00" );

/**
 * @desc MySQL abstraction layer
 * @throws PDOException
 */
class pg
{
	protected static $_connected = false;

	/** @var PDO */
	protected static $_pdo;

	protected static $_addRowsContainer = [ ];

	protected static $_inTransaction = false;

	protected static $_config;

	const IGNORE  = 1;
	const REPLACE = 2;

	private $_value;


	public static function config( array $options )
	{
		foreach ( [ 'host', 'database', 'user', 'pass', 'error_handler', 'on_connect' ] as $option ) {
			if ( array_key_exists( $option, $options ) ) {
				static::$_config[ $option ] = $options[ $option ];
			}
		}
	}

	public static function connect( $options = null )
	{
		func_get_args() and static::config( $options );

		if ( static::$_connected === false ) {
			if ( !isset( static::$_config['host'], static::$_config['database'],
				static::$_config['user'], static::$_config['pass'] ) ) {
				throw new errException( 'Postgres connection not configured.' );
			}

			if ( empty( static::$_config['error_handler'] ) ) {
				static::$_config['error_handler'] = __CLASS__ . '::_exceptionHandler';
			}

			try {
				# todo set server timezone to UTC, set user timezone in postgres as soon as possible, store timestamps..
				# todo .. with TZ information: http://stackoverflow.com/a/6158432/179104
				static::$_pdo = new PDO(
					sprintf(
						"pgsql:host=%s;dbname=%s;user=%s;password=%s;",
						static::$_config['host'],
						static::$_config['database'],
						static::$_config['user'],
						static::$_config['pass']
					)
				);
			} catch ( PDOException $e ) {
				throw new errException( "CAN'T CONNECT TO POSTGRES: " . $e->getMessage(), get_defined_vars() );
			}

			static::$_pdo->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
			static::$_connected = true;

			if ( !empty( static::$_config['on_connect'] ) ) {
				call_user_func( static::$_config['on_connect'] );
			}
		}

		return static::$_connected;
	}


	/**
	 * Close connection to database
	 */
	public static function close()
	{
		if ( static::$_connected === true ) {

			static::$_pdo              = null;
			static::$_connected        = false;
			static::$_addRowsContainer = [ ];
		}
	}

	/**
	 * sets custom query error handler;
	 *
	 * @see pg::_exceptionHandler()
	 *
	 * @param callable $handler
	 */
	public static function setErrorHandler( $handler )
	{
		static::$_config['error_handler'] = $handler;
	}

	/**
	 * handles PDOException raised due to errors in query
	 *
	 * @param string       $query
	 * @param mixed        $boundValues
	 * @param PDOException $e
	 */
	private static function _handleError( $query = null, $boundValues = null, PDOException $e = null )
	{
		$boundQuery = static::bindQuery(
			$query,
			$boundValues,
			isset( $e->errorInfo[2] ) ? $e->errorInfo[2] : $e->getMessage()
		);

		call_user_func( static::$_config['error_handler'], $e, $query, $boundValues, $boundQuery );
	}

	/**
	 * default query error handler
	 *
	 * @param PDOException $e
	 * @param string       $query
	 * @param array        $boundValues
	 * @param string       $boundQuery
	 *
	 * @throws errException
	 */

	private static function _exceptionHandler( PDOException $e, $query, $boundValues, $boundQuery )
	{
		$errorMessage = isset( $e->errorInfo[2] ) ? $e->errorInfo[2] : $e->getMessage();

		$trace         = debug_backtrace( true );
		$internalSteps = 0;
		foreach ( $trace as $step ) {
			if ( isset( $step['file'] ) && $step['file'] === __FILE__ ) {
				$internalSteps++;
			}
		}
		if ( $internalSteps ) {
			$trace = array_slice( $trace, $internalSteps + 1 );
		}

		$exception = new errException( $errorMessage );
		$exception->trace( $trace )
		          ->details( [ 'Query' => $boundQuery, 'Bound values' => $boundValues ] )
		          ->type( 'PostgreSQL' );

		throw $exception;
	}

	/***
	 *    ##       #### ######## ######## ########     ###    ##
	 *    ##        ##     ##    ##       ##     ##   ## ##   ##
	 *    ##        ##     ##    ##       ##     ##  ##   ##  ##
	 *    ##        ##     ##    ######   ########  ##     ## ##
	 *    ##        ##     ##    ##       ##   ##   ######### ##
	 *    ##        ##     ##    ##       ##    ##  ##     ## ##
	 *    ######## ####    ##    ######## ##     ## ##     ## ########
	 */


	private function __construct( $value, $param = null )
	{
		is_array( $param ) or $param = (array) $param;

		// replace each subsequent ? with an element from the array
		while ( strpos( $value, '?' ) !== false ) {
			if ( empty( $param ) ) {
				throw new errException( 'Param has more members than query params' );
			}

			// escape the needed value and replace the first
			$replace = static::$_pdo->quote( array_shift( $param ) );
			$value   = preg_replace( '/\?/', $replace, $value, 1 );
		}

		$this->_value = $value;
	}

	public static function literal( $value, $param = null )
	{
		static::$_connected or static::connect();
		return new static ( $value, $param );
	}

	private function _get()
	{
		return $this->_value;
	}

	public function __toString()
	{
		return $this->_get();
	}


	private static function _wrapValue( $key, $value, $errorMessage )
	{
		$value = $errorMessage === true || is_numeric( $value )
			? $value
			: "'{$value}'";

		return $errorMessage === true
			? static::$_pdo->quote( $value )
			: "<abbr title=\"{$key}\">{$value}</abbr>";
	}

	/**
	 * emulates preparing query with parameters
	 *
	 * @param      $query
	 * @param      $boundValues
	 * @param bool $errorMessage pass string message in case you want the error emphasized, otherwise pass true|void
	 *
	 * @return mixed
	 */
	public static function bindQuery( $query, $boundValues, $errorMessage = true )
	{
		if ( $boundValues === PG_NO_BOUND || $boundValues === [ ] ) {
			return static::_highlightErrors( $query, $errorMessage );
		}

		$replacements = [ ];
		if ( !is_array( $boundValues ) ) {
			$boundValues = (array) $boundValues;
		}

		# build a regular expression for each parameter
		foreach ( $boundValues as $key => $value ) {

			if ( is_string( $key ) ) {
				$key    = ':' . $key;
				$strlen = strlen( $key );

				preg_match_all( "/\\W({$key})\\W/", $query, $matches, PREG_OFFSET_CAPTURE );
				if ( isset( $matches[1] ) ) {
					$offset = 0;
					foreach ( $matches[1] as $ps ) {
						$pos = $ps[1] + $offset;
						$offset -= $strlen - 2;

						$query = substr_replace( $query, "##", $pos, $strlen );


						$n = [ ];
						foreach ( $replacements as $k => $r ) {
							if ( $k > $pos ) {
								$k -= $strlen - 2;
							}
							$n[ $k ] = $r;
						}
						$replacements = $n;

						$replacements[ $pos ] = static::_wrapValue( $key, $value, $errorMessage );
					}
				}
			} else {
				$key = '?';
				$pos = 0;

				$pos = strpos( $query, $key, $pos );
				if ( $pos !== false ) {
					$query = substr_replace( $query, "##", $pos, strlen( $key ) );
					$pos += 2;

					$replacements[ $pos ] = static::_wrapValue( $key, $value, $errorMessage );
				}
			}
		}

		$query = static::_highlightErrors( $query, $errorMessage );

		if ( !empty( $replacements ) ) {
			ksort( $replacements );
			$query = preg_replace( array_fill( 0, sizeof( $replacements ), '/##/' ), $replacements, $query, 1 );
		}

		return $query;
	}

	private static function _highlightErrors( $query, $errorMessage )
	{
		if ( $errorMessage === true ) return $query;

		preg_match( "#at character (\\d+)#", $errorMessage, $matches );
		if ( isset( $matches[1] ) ) {
			$errorAt = $matches[1] - 1;

			preg_match( '#"([^"]+)"#', $errorMessage, $matches );
			$offset = ( isset( $matches[1] ) )
				? strlen( $matches[1] )
				: 5;

			$query = substr_replace( $query, '</span>', $errorAt + $offset, 0 );
			$query = substr_replace( $query, '<span class="sql-error">', $errorAt, 0 );
			return $query;
		}
		return $query;
	}


	/**
	 * replaces empty strings with DEFAULT
	 *
	 * @static
	 *
	 * @param mixed $value
	 *
	 * @return mixed
	 */
	public static function defaultify( $value )
	{
		if ( is_array( $value ) ) {
			foreach ( $value as &$v ) {
				$v === '' and $v = static::literal( 'DEFAULT' );
			}

			return $value;
		}
		return $value === '' ? static::literal( 'DEFAULT' ) : $value;
	}

	/***
	 *    ##     ## ######## ######## ##     ##  #######  ########   ######
	 *    ###   ### ##          ##    ##     ## ##     ## ##     ## ##    ##
	 *    #### #### ##          ##    ##     ## ##     ## ##     ## ##
	 *    ## ### ## ######      ##    ######### ##     ## ##     ##  ######
	 *    ##     ## ##          ##    ##     ## ##     ## ##     ##       ##
	 *    ##     ## ##          ##    ##     ## ##     ## ##     ## ##    ##
	 *    ##     ## ########    ##    ##     ##  #######  ########   ######
	 */

	/**
	 * execute SQL query that returns no rows
	 *
	 * @param string $query
	 * @param mixed  $boundValues
	 *
	 * @throws PDOException
	 * @return bool|int affected number of rows by this query|false on failure
	 */

	public static function exec( $query, $boundValues = PG_NO_BOUND )
	{
		if ( empty( $query ) ) {

			throw new PDOException( 'pg::exec empty query' );

		} elseif ( $boundValues !== PG_NO_BOUND ) {

			return static::query( $query, $boundValues )->rowCount();

		} else {

			try {
				static::$_connected or static::connect();

				return static::$_pdo->exec( $query );

			} catch ( PDOException $e ) {

				static::_handleError( $query, $boundValues, $e );

			}
		}
	}

	/**
	 * decides whether to prepare a statement (based on amount of parameters) and executes a query
	 *
	 * @param string $query       SQL query to execute
	 * @param mixed  $boundValues values to bind: accepts array or single value
	 *
	 * @throws PDOException
	 * @throws errException
	 * @return PDOstatement
	 */

	public static function query( $query, $boundValues = PG_NO_BOUND )
	{
		static::$_connected or static::connect();
		$statement = false;
		if ( empty( $query ) ) {

			throw new PDOException( 'pg::query() empty' );

		} elseif ( $boundValues !== PG_NO_BOUND ) {
			if ( !isset( $boundValues ) ) {
				$boundValues = [ null ];
			} elseif ( !is_array( $boundValues ) ) {
				$boundValues = (array) $boundValues;
			}

			$isAssoc = Arr::isAssoc( $boundValues );

			$arrIndex        = 0;
			$queryOffset     = 0;
			$processedValues = $boundValues; # php (should) do this on its own, but lets avoid confusion
			foreach ( $boundValues as $key => &$val ) {

				if ( is_array( $val ) ) {
					$bug = true;

					if ( $isAssoc ) {

						$regex = "[\\sin\\s*\\(\\s*:{$key}\\s*\\)]i";

						if ( preg_match_all( $regex, $query, $matches, PREG_OFFSET_CAPTURE ) ) {

							$offset = 0;
							foreach ( $matches[0] as $match ) {
								$replacement = '';
								$newVal      = [ ];
								foreach ( $val as $k => $v ) {
									if ( $v === true || $v === false ) {
										$v = $v ? 't' : 'f';
									}

									$newKey = "{$key}__{$k}";
									$replacement .= ':' . $newKey . ',';
									$newVal[ $newKey ] = $v;
								}
								$replacement = " IN (" . substr( $replacement, 0, -1 ) . ")";

								$query       = substr_replace(
									$query,
									$replacement,
									$match[1] + $offset,
									strlen( $match[0] )
								);
								$queryOffset = $match[1] + $offset + strlen( $match[0] );
								$offset += strlen( $replacement ) - strlen( $match[0] );

								unset( $processedValues[ $key ] );
								$processedValues = array_merge( $processedValues, $newVal );
							}
							$bug = false;
						}

					} else {

						$regex = "[\\sin\\s*\\(\\s*\\?\\s*\\)]i";
						if ( preg_match( $regex, $query, $matches, PREG_OFFSET_CAPTURE, $queryOffset ) ) {
							foreach ( $val as $k => &$v ) {
								if ( $v === true || $v === false ) {
									$v = $v ? 't' : 'f';
								}
							}
							unset( $v );

							$replacement = substr( str_repeat( '?,', count( $val ) ), 0, -1 );
							$query       = substr_replace(
								$query,
								" IN ($replacement)",
								$matches[0][1],
								strlen( $matches[0][0] )
							);
							$queryOffset = $matches[0][1] + strlen( $matches[0][0] );
							array_splice( $processedValues, $arrIndex, 1, $val );
							$arrIndex += count( $val ) - 1;

							$bug = false;
						}

					}

					if ( $bug ) throw new errException( 'Nested array provided to bind to a query.', get_defined_vars() );
				} elseif ( $val === true || $val === false ) {
					$processedValues[ $key ] = $val ? 't' : 'f';
				}
				$arrIndex++;
			}

			try {

				/** @var $statement PDOStatement */
				$statement = static::$_pdo->prepare( $query );

				$statement->execute( $processedValues );

			} catch ( PDOException $e ) {
				static::_handleError( $query, $processedValues, $e );
			}

		} else {

			try {

				$statement = static::$_pdo->query( $query );

			} catch ( PDOException $e ) {

				static::_handleError( $query, $boundValues, $e );

			}
		}

		return $statement;
	}


	/**
	 * Adds/replaces one row. Action depends on the
	 *
	 * @param string       $table
	 * @param array        $data    single level associative array of `column=>value` pairs
	 * @param string|false $primary pass false if no autoincrement column present
	 *
	 * @throws errException
	 * @return int new primary key value
	 */
	public static function addRow( $table, $data = [ ], $primary = 'id' )
	{
		$query = 'INSERT INTO ' . $table;

		// build key and value clauses
		if ( !empty( $data ) ) {
			$keyQuery = '';
			$valQuery = '';

			foreach ( $data as $key => $value ) {
				$keyQuery .= "\"{$key}\",";

				if ( isset( $value ) && !is_scalar( $value ) ) {
					if ( $value instanceof self ) {
						$valQuery .= $value->_get() . ',';

						unset( $data[ $key ] );
					} else {
						// it's an unknown object or an array - most definitely a bug
						throw new errException( 'Passed value to addRow is not string or numeric', $table, $data );
					}
				} else {
					$valQuery .= ":{$key},";
				}
			}
			$query .= '(' . substr( $keyQuery, 0, -1 ) . ')VALUES(' . substr( $valQuery, 0, -1 ) . ')';
		} else {
			$query .= ' DEFAULT VALUES';
		}

		if ( $primary !== false ) {
			$query .= ' RETURNING ' . $primary;
		}

		return static::query( $query, $data )->fetchColumn( 0 );
	}

	/**
	 * @static
	 *
	 * @param string       $table
	 * @param string|array $whereClause - no 'WHERE' keyword, just condition, accepts pairs of values too
	 * @param mixed        $boundValues - these are for whereClause
	 *
	 * @throws ErrException
	 * @throws errException
	 * @throws errException
	 * @return int number of deleted rows
	 */
	public static function deleteRows( $table, $whereClause = null, $boundValues = PG_NO_BOUND )
	{
		if ( empty( $table ) ) {
			throw new ErrException( 'provide a table name', get_defined_vars() );
		}

		if ( is_array( $whereClause ) ) {
			$columns     = $whereClause;
			$whereClause = '';
			if ( $boundValues !== PG_NO_BOUND ) {
				throw new errException( 'specify bound values in where clause if you pass it as array' );
			}
			$boundValues = $columns;


			$assoc = Arr::isAssoc( $columns );

			foreach ( $columns as $key => $value ) {

				if ( isset( $value ) && !is_scalar( $value ) ) {
					if ( $value instanceof self ) {
						$whereClause[] = "\"{$key}\"=" . $value->_get();

						unset( $boundValues[ $key ] );
					} else {
						// it's an unknown object or an array - most definitely a bug
						throw new errException( 'Passed value to deleteRows is not string or numeric', $key, $value );
					}
				} else {
					$param = $assoc ? ":{$key}" : "?";

					$whereClause[] = "\"{$key}\"={$param}";
				}

			}

			$whereClause = implode( ' AND ', $whereClause );
		}

		$whereClause and $whereClause = 'WHERE ' . $whereClause;

		$query = "DELETE FROM {$table} {$whereClause}";


		return static::exec( $query, $boundValues );
	}

	/**
	 * @static
	 *
	 * @param string       $table
	 * @param array|pg[]   $data
	 * @param array|string $whereClause note that this only supports question marks as sql parameters. I.e. no :id ones
	 * @param mixed        $boundValues
	 *
	 * @throws ErrException
	 * @throws errException
	 * @return bool|int number of rows affected
	 */
	public static function updateTable( $table, $data = [ ], $whereClause = null, $boundValues = PG_NO_BOUND )
	{
		if ( empty( $data ) ) {
			throw new ErrException( 'no data to update', get_defined_vars() );
		}

		$query        = "UPDATE {$table} SET ";
		$updateClause = [ ];
		foreach ( $data as $key => &$value ) {


			if ( isset( $value ) && !is_scalar( $value ) ) { // if not null and not scalar
				if ( $value instanceof self ) {
					$updateClause[] = "\"{$key}\"=" . $value->_get();

					unset( $data[ $key ] );
				} else {
					// it's an unknown object or an array - most definitely a bug
					throw new errException( 'Passed value to updateTable is not string or numeric', $key, $value );
				}
			} else {
				if ( is_bool( $value ) ) $value = (int) $value;

				$updateClause[] = "\"{$key}\"=?";
			}
		}
		unset( $value );

		$query .= implode( ',', $updateClause );
		$data = array_values( $data );

		if ( isset( $whereClause ) ) {

			if ( $boundValues !== PG_NO_BOUND ) {
				if ( is_array( $boundValues ) ) {
					if ( Arr::isAssoc( $boundValues ) ) {
						throw new errException( 'Named parameters are not supported in updateTable, sorry' );
					}
					$data = array_merge( $data, $boundValues );
				} else {
					array_push( $data, $boundValues );
				}
			}

			if ( is_array( $whereClause ) ) {
				if ( $boundValues !== PG_NO_BOUND ) {
					throw new errException( 'specify bound values in where clause if you pass it as array' );
				}
				if ( !Arr::isAssoc( $whereClause ) ) {
					throw new errException( 'if where clause is array it must be associative' );
				}

				$columns     = $whereClause;
				$whereClause = [ ];

				foreach ( $columns as $key => $value ) {

					if ( isset( $value ) && !is_scalar( $value ) ) {
						if ( $value instanceof self ) {
							$whereClause[] = "\"{$key}\"=" . $value->_get();
							unset( $columns[ $key ] );
						} else {
							// it's an unknown object or an array - most definitely a bug
							throw new errException( 'Passed value to deleteRows is not string or numeric', $key, $value );
						}
					} else {
						$whereClause[] = "\"{$key}\"=?";
					}

				}

				$whereClause = implode( ' AND ', $whereClause );
				$data        = array_merge( $data, array_values( $columns ) );
			}

			$query .= " WHERE {$whereClause}";
		}

		return static::exec( $query, $data );
	}

	/**
	 * Create lots of new rows, prioritise performance;
	 *
	 * foreach ($data as $row) {
	 *     mysql::addRows('table_name', array('id'=>$row['id']));
	 * }
	 * mysql::addRows('table_name');
	 *
	 * @param string    $table
	 * @param mixed     $data
	 *                         array:
	 *                         data to be inserted, may be indexed or numeric. The latter only if you are passing all
	 *                         present table fields *AND* in the correct order. (For eg. quite safe in many-to-many
	 *                         relationship tables). NULL: the operation on passed table is cancelled. Otherwise it
	 *                         will be performed on shutdown void: the operation is performed explicitly
	 * @param int|array $extra mysql::REPLACE or mysql::IGNORE
	 *
	 * @throws errException
	 * @return int|false rows inserted/false on fail
	 */
	public static function addRows( $table, $data = [ ], $extra = null )
	{
		$container = &static::$_addRowsContainer[ $table ];

		if ( func_num_args() === 1 ) {
			// check if it's the table name and commit if so (the last step)
			if ( isset( $container ) ) {

				$valQuery      = '';
				$keyQuery      = '';
				$keysGenerated = false;
				foreach ( $container['values'] as $row ) {
					foreach ( $row as $key => $value ) {
						$keysGenerated or $keyQuery .= "\"{$key}\",";

						if ( isset( $value ) && !is_scalar( $value ) ) {
							if ( $value instanceof self ) {
								$valQuery .= $value->_get() . ',';
							} else {
								// it's an unknown object or an array - most definitely a bug
								throw new errException( 'Passed value to updateTable is not string or numeric',
									$key, $value
								);
							}
						} else {
							if ( is_bool( $value ) ) $value = (int) $value;
							$valQuery .= static::$_pdo->quote( $value ) . ',';
						}
					}
					$valQuery      = substr( $valQuery, 0, -1 ) . '),(';
					$keysGenerated = true;
				}
				$valQuery = substr( $valQuery, 0, -3 );
				$keyQuery = substr( $keyQuery, 0, -1 );


				$query = "INSERT INTO\"{$table}\"({$keyQuery})VALUES({$valQuery})";

				$affected = static::exec( $query );
				unset( static::$_addRowsContainer[ $table ] );
				return $affected;
			} else {
				// todo this is a probable bug
				return false;
			}

		} elseif ( $data === null ) {
			// if NULL is passed, reset the values to be inserted

			unset( static::$_addRowsContainer[ $table ] );

			return;
		} elseif ( isset( $container ) ) {
			// add more values to insert to a started operation (means the same table is called a second or later time)

			$container['values'][] = $data;

		} else {
			// it's the first time this table is called; initialize

			$container['values'][] = $data;

			// add the rows at the end of execution unless recalled
			// shutdown::getInstance()->register(array('mysql', 'addRows'), $table);
		}
	}

	public static function exists( $table, $whereClause = null, $boundValues = PG_NO_BOUND )
	{
		if ( empty( $whereClause ) ) {

		} elseif ( is_array( $whereClause ) ) {
			$boundValues = array_values( $whereClause );
			$whereClause = ' WHERE ' . implode( ' = ? AND ', array_keys( $whereClause ) ) . ' = ?';
		} else {
			$whereClause and $whereClause = ' WHERE ' . $whereClause;
		}

		return (bool) static::query( "SELECT 1 FROM {$table}{$whereClause} LIMIT 1", $boundValues )->fetchColumn();
	}

	public static function getRow( $query, $boundValues = PG_NO_BOUND )
	{
		return static::query( $query, $boundValues )->fetch( PDO::FETCH_ASSOC );
	}

	/**
	 * @desc Get single value from query as string
	 *
	 * @param string $query
	 * @param mixed  $boundValues if bound values is given, the query will be prepared and executed with these values
	 *
	 * @return mixed false if no such row found
	 */

	public static function getOne( $query, $boundValues = PG_NO_BOUND )
	{
		return static::query( $query, $boundValues )->fetchColumn( 0 );
	}

	/**
	 * @desc Get last executed query results as two-dimensional array
	 *
	 * @param string $query
	 * @param mixed  $boundValues if bound values is given, the query will be prepared and executed with these values
	 *
	 * @return array of fetched results
	 */
	public static function getAll( $query, $boundValues = PG_NO_BOUND )
	{
		return static::query( $query, $boundValues )->fetchAll( PDO::FETCH_ASSOC );
	}

	/**
	 * @static Get query results as two-dimensional array as defined by format
	 *
	 *
	 * example:
	 *  format: "id=>value"
	 *  query result:
	 *  __________
	 *  |id|value|
	 *  ----------
	 *  | 5|apple|
	 *  ----------
	 *
	 *  returns array( '5' => 'apple' )
	 *
	 *
	 * example2:
	 *  format: "id=>value;type"
	 * !!! OR
	 *  format: "id=>*"
	 *
	 *  query result:
	 *  ________________
	 *  |id|value|type |
	 *  ----------------
	 *  | 5|apple|fruit|
	 *  ----------------
	 *
	 *  returns array( '5' => array( 'value' => 'apple', 'type' => 'fruit' ) )
	 *
	 * example3:
	 *  format: "id[]=>value;type"
	 * !!! OR
	 *  format: "id[]=>*"
	 *  query result:
	 *  _________________
	 *  |id|value|type  |
	 *  -----------------
	 *  | 5|apple|fruit |
	 *  | 5|cat  |mammal|
	 *  | 6|pear |fruit |
	 *  -----------------
	 *
	 *  returns array( '5' => array(
	 *                              0 => array( 'value' => 'apple', 'type' => 'fruit'  ),
	 *                              1 => array( 'value' => 'cat'  , 'type' => 'mammal' ),
	 *                          ),
	 *                 '6' => array(
	 *                              0 => array( 'value' => 'pear' , 'type' => 'fruit'  ),
	 *                          ),
	 *              )
	 * example4:
	 *  format: "id[type]=>value" // you can also nest braces like this: key1[key2][key3][]
	 * !!! OR
	 *  format: "id[type]=>*"
	 *  query result:
	 *  _________________
	 *  |id|value|type  |
	 *  -----------------
	 *  | 5|apple|fruit |
	 *  | 5|cat  |mammal|
	 *  | 5|rhino|mammal|
	 *  | 6|pear |fruit |
	 *  -----------------
	 *
	 *  returns array( '5' => array(
	 *                              'fruit' => 'pear',
	 *                              'mammal' => 'rhino', // if more than one value is found with same key[key2]=>... ,
	 *                                                   // the last value is returned without warning, to prevent this
	 *                                                   // use key[key2][]=>..
	 *                          ),
	 *                 '6' => array(
	 *                              'fruit' => 'pear'
	 *                          ),
	 *              )
	 *
	 *
	 * @param string $query
	 * @param string $format
	 * @param mixed  $boundValues
	 * @param bool   $keepKey
	 *
	 * @throws ErrException
	 * @return array|false
	 */

	public static function getAsAssoc( $query, $format, $boundValues = PG_NO_BOUND, $keepKey = false )
	{
		preg_match( '#([^=\[]+)((?:\[(?:[^\]]*)\])*)=>(.+)#', $format, $matches );

		try {
			list( , $key, $braces, $columns ) = $matches;
		} catch ( Exception $e ) {
			throw new ErrException( 'Invalid format pattern', get_defined_vars() );
		}

		if ( $braces ) {
			preg_match_all( '#\[([^\]]*)\]#', $braces, $matches );
			$nestedKeys = $matches[1];
		}

		if ( $PdoStatement = static::query( $query, $boundValues ) ) {

			$values = $columns === '*' ? $columns : explode( ';', $columns );

			$hasMultipleValues = $values === '*' || isset( $values[1] );

			$rows = [ ];
			while ( $result = $PdoStatement->fetch( PDO::FETCH_ASSOC ) ) {
				if ( $hasMultipleValues ) {

					if ( $values === '*' ) {
						$row = $result;
						if ( !$keepKey ) {
							unset( $row[ $key ] );
						}
					} else {
						foreach ( $values as $v ) {
							$row[ $v ] = $result[ $v ];
						}
					}

				} else {
					$row = $result[ $columns ];
				}

				if ( $braces ) {
					isset( $rows[ $result[ $key ] ] ) or $rows[ $result[ $key ] ] = [ ];

					$cont = &$rows[ $result[ $key ] ];

					foreach ( $nestedKeys as $nestedKey ) {

						if ( $values === '*' ) {
							if ( !$keepKey ) {
								unset( $row[ $nestedKey ] );
							}
						}

						if ( $nestedKey ) {
							isset( $cont[ $result[ $nestedKey ] ] ) or $cont[ $result[ $nestedKey ] ] = [ ];
							$cont = &$cont[ $result[ $nestedKey ] ];
						} else {
							$cont[] = [ ];
							$cont   = &$cont[ Arr::last( $cont, true ) ];
						}
					}


					$cont = $row;
				} else {
					$rows[ $result[ $key ] ] = $row;

				}


			}
			return $rows;
		}

		return false;
	}

	/**
	 * Get a single dimension array, is usefull only for single column select
	 *
	 * @static
	 *
	 * @param string       $query
	 * @param array|string $boundValues
	 *
	 * @return array|false fetched results or false on error
	 */

	public static function getSingleColumn( $query, $boundValues = PG_NO_BOUND )
	{
		if ( $resource = static::query( $query, $boundValues ) ) {
			$result = [ ];
			while ( ( $record = $resource->fetchColumn( 0 ) ) !== false ) {
				$result[] = $record;
			}
			return $result;
		}
		return false;
	}

	/**
	 * @static returns number of rows returned in query
	 *
	 * @param  $query
	 * @param  $boundValues
	 *
	 * @return int|NULL
	 */
	public static function countRows( $query, $boundValues = PG_NO_BOUND )
	{
		return static::query( $query, $boundValues )->rowCount();
	}


	public static function metaGetColumns( $table )
	{
		return static::getSingleColumn(
			"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?",
			$table
		);
	}


	public static function tableExists( $tableName )
	{
		$prevErrMode = static::$_pdo->getAttribute( PDO::ATTR_ERRMODE );
		static::$_pdo->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
		try {
			static::getOne( "SELECT 1 FROM {$tableName} LIMIT 1" );
			$exists = true;
		} catch ( Exception $e ) {
			$exists = false;
		}
		static::$_pdo->setAttribute( PDO::ATTR_ERRMODE, $prevErrMode );
		return $exists;
	}

	public static function schemaExists( $schema )
	{
		return pg::getOne(
			"SELECT exists (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?)",
			$schema
		);
	}


	/* ************************
	 * DIRECT WRAPPERS TO PDO *
	 ************************ */

	public static function beginTransaction()
	{
		static::$_connected or static::connect();

		if ( static::$_inTransaction ) {
			return;
		}

		static::$_inTransaction = true;

		static::$_pdo->beginTransaction();
	}

	public static function commit()
	{
		if ( !static::$_inTransaction ) {
			return;
		}

		static::$_inTransaction = false;

		static::$_pdo->commit();
	}

	public static function rollBack()
	{
		static::$_pdo->rollBack();
	}

	public static function lastInsertId()
	{
		return static::$_pdo->lastInsertId();
	}
}