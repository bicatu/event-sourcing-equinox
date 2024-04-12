import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { CachingStrategy, Category, Codec, Decider, ICodec, StreamId, Uuid } from "@equinox-js/core";
import { AccessStrategy, DynamoStoreCategory, DynamoStoreClient, DynamoStoreContext, QueryOptions, TipOptions } from "@equinox-js/dynamo-store";
import { AggregateStreamOptions, AggregateStreamResult, EventStore, Event, ReadStreamOptions, ReadStreamResult, AppendToStreamOptions, AppendToStreamResult } from "@event-driven-io/emmett";

export type AccountId = Uuid.Uuid<"AccountId">
export const AccountId = Uuid.create<"AccountId">()

const category = "Account";

export const codec: ICodec<AccountEvent, string> = {
  decode(event): AccountEvent | undefined {
    const data = JSON.parse(event.data || "{}");
    switch (event.type) {
      case "Opened": return { type: event.type, data: { status: data.status, name: data.name, amount: data.amount } }
      case "Deposited": return { type: event.type, data: { amount: data.amount } }
      case "Withdrawn": return { type: event.type, data: { amount: data.amount } }
    }
  },
  encode(event) {
    const data = "data" in event ? JSON.stringify(event.data) : undefined;
    return { type: event.type, data }
  }
}

export type AccountStatus = "NonExistent" | "Open" | "Closed";

export type State = {
  status: AccountStatus;
  name: string;
  amount: number;
}

export const initialState: State = { status: 'NonExistent', name: "", amount: 0 }


export type AccountOpened = Event<'Opened', { status: AccountStatus; name: string; amount: number; }>;
export type MoneyDeposited = Event<'Deposited', { amount: number; }>;
export type MoneyWithdrawn = Event<'Withdrawn', { amount: number; }>;


export type AccountEvent = AccountOpened | MoneyDeposited | MoneyWithdrawn;

export const evolve = (state: State, event: AccountEvent): State => {
  switch (event.type) {
    case "Opened":
      return {
        ...state,
        status: "Open",
        name: event.data.name
      };
    case "Deposited":
      return {
        ...state,
        amount: state.amount + event.data.amount
      };
    case "Withdrawn":
      return {
        ...state,
        amount: state.amount - event.data.amount
      }
    default:
      return state;
  }
}

export const fold = (state: State, events: AccountEvent[]) => events.reduce(evolve, state)

// ==========================================

class Service {
  constructor(private readonly resolve: resolveType) { }

  public decider(accountId: AccountId): Decider<AccountEvent, State> {
    return this.resolve(accountId);
  }
}

const dynamoDB = new DynamoDB({ region: "local", endpoint: "http://localhost:8000" });
const client = new DynamoStoreClient(dynamoDB);
const context = new DynamoStoreContext({
  client,
  tableName: "equinox-js-example",
  tip: TipOptions.create({}),
  query: QueryOptions.create({}),
});

const access = AccessStrategy.Unoptimized();
const dynamoStoreCategory = DynamoStoreCategory.create(context, category, Codec.compress(codec), fold, initialState, CachingStrategy.NoCache(), access);
type resolveType = (accountId: AccountId) => Decider<AccountEvent, State>;

const accountId = AccountId.create();
const resolve = () => Decider.forStream(dynamoStoreCategory, StreamId.create(accountId.toString()), null);
const service = new Service(resolve);
const decider = service.decider(accountId);


// ========================================

export class EquinoxService {
  constructor(public resolve: resolveType) { }

  public decider(accountId: AccountId): Decider<AccountEvent, State> {
    return this.resolve(accountId);
  }
}

export const getDynamoDbEventStore = (mdynamoStoreCategory: Category<Event, State>): EventStore => {
  return {
    async aggregateStream<State, EventType extends Event>(streamName: string, options: AggregateStreamOptions<State, EventType>,): Promise<AggregateStreamResult<State> | null> {
      const { evolve, getInitialState, read } = options;

      let state = getInitialState();


      let currentStreamVersion = BigInt(0);

      return { currentStreamVersion: currentStreamVersion, state }
    },

    readStream: async <EventType extends Event>(streamName: string, options?: ReadStreamOptions,): Promise<ReadStreamResult<EventType>> => {
      const events: EventType[] = [];

      return { currentStreamVersion: BigInt(events.length), events };
    },

    appendToStream: async <EventType extends Event>(streamName: string, events: EventType[], options?: AppendToStreamOptions,): Promise<AppendToStreamResult> => {
      // 1st issue: Emmett CommandHandler creates the streamName based on a provided function that uses the aggregateId 
      // Right now since I am just calling the event store directly I'm assuming the streamName as the aggregateId
      let resolve = () => Decider.forStream(mdynamoStoreCategory, StreamId.create(streamName), null);
      const service = new EquinoxService(resolve);
      // connected issue: since the decider expects an accountId, I'm parsing the streamName back to an AccountId
      const decider = service.decider(AccountId.parse(streamName));

      //@ts-ignore
      decider.transact(() => events);

      // 2nd issue: How to get the next expect stream version?
      return { nextExpectedStreamVersion: BigInt(events.length) };
    }
  };
};

// ========================

// Just using the Emmett Event definition for now
// (async () => {
//   console.log(accountId);
//   await decider.transact(() =>[{ type: "Opened", data: { status: "Open", name: "John Doe", amount: 0 } }]);
//   await decider.transact(() =>[{ type: "Deposited", data: { amount: 100 } }]);
//   await decider.transact(() =>[{ type: "Deposited", data: { amount: 15 } }]);
//   await decider.transact(() =>[{ type: "Deposited", data: { amount: 154 } }]);
// })();


// Using the Emmett EventStore definition
// For some reason if I do separate calls to appendToStream, the last event complains with MaxResyncsExhaustedException [Error]: Concurrency violation; aborting after 3 attempts.
(async () => {
  const store = getDynamoDbEventStore(dynamoStoreCategory);
  console.log(accountId);
  await store.appendToStream(accountId.toString(), [{ type: "Opened", data: { status: "Open", name: "John Do e", amount: 0 } }, { type: "Deposited", data: { amount: 100 } }, { type: "Deposited", data: { amount: 140 } }, { type: "Deposited", data: { amount: 90 } }]);
})();