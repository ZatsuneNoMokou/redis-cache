import {createClient, RedisClientOptions, RedisClientType} from 'redis';
import {TObject} from "@sinclair/typebox";
import {TypeCheck, TypeCompiler} from '@sinclair/typebox/compiler';

export interface RedisCacheOptions {
	prefix: string
	scanCount: number
	schema: TObject
	ttl: number
}

export class RedisCache<T> {
	#schemaCompiler: TypeCheck<TObject>;
	private constructor(private opts:RedisCacheOptions, private client:RedisClientType) {
		if (opts.ttl <= 0 || Number.isNaN(opts.ttl)) {
			throw new Error('UNEXPECTED_TTL');
		}
		this.#schemaCompiler = TypeCompiler.Compile(opts.schema)
	}

	static new<T>(opts:RedisCacheOptions, redisOpts:RedisClientOptions) : RedisCache<T> {
		const client = (<RedisClientType>createClient({
			...redisOpts,
		}))
		return new RedisCache<T>(opts, client);
	}

	get prefix(): string {
		return this.opts.prefix;
	}

	get ttl(): number {
		return this.opts.ttl;
	}

	get scanCount(): number {
		return this.opts.scanCount;
	}

	async get(id:string): Promise<T|undefined> {
		const key = this.prefix + id;
		let value : T|null = null;
		try {
			const data = await this.client.get(key);
			if (!data) {
				return;
			}
			value = <T>(JSON.parse(data));
		} catch (err) {
			throw new Error(err);
		}

		if (value === null) {
			return undefined;
		}

		if (!this.#schemaCompiler.Check(value)) {
			throw new Error('SCHEMA_CHECK');
		}
		return value;
	}

	async set(id:string, value:T) {
		const key = this.prefix + id;
		try {
			const val = JSON.stringify(value);
			await this.client.set(key, val, { EX: this.ttl });
			return;
		} catch (err) {
			throw new Error(err);
		}
	}

	async has(id:string): Promise<boolean> {
		const key = this.prefix + id;
		return (await this.client.exists(key)) === 1
	}

	async delete(id:string) {
		const key = this.prefix + id;
		try {
			await this.client.del([key]);
			return true;
		} catch (err) {
			throw new Error(err);
		}
	}

	async* keys() : AsyncIterableIterator<string> {
		const pattern = this.prefix + "*",
			len = this.prefix.length
		;

		for await (let key of this.client.scanIterator({ MATCH: pattern, COUNT: this.scanCount })) {
			yield key.substring(len);
		}
	}

	async size() {
		const pattern = this.prefix + "*";
		try {
			const keys = await this.client.keys(pattern);
			return keys.length;
		} catch (err) {
			throw new Error(err);
		}
	}

	async* [Symbol.asyncIterator]() : AsyncIterableIterator<[string,T]> {
		const pattern = this.prefix + "*",
			len = this.prefix.length
		;

		for await (let key of this.client.scanIterator({ MATCH: pattern, COUNT: this.scanCount })) {
			const value = await this.get(key);
			if (!value) {
				console.warn(`COULD_NOT_FIND "${key}"`);
				continue;
			}
			if (!this.#schemaCompiler.Check(value)) {
				console.warn(`SCHEMA_CHECK "${key}"`);
				continue;
			}
			yield [key.substring(len), value];
		}
	}
}
