import { Static, Type, TSchema } from '@sinclair/typebox';
import { Feature } from '@tak-ps/node-cot'
import moment from 'moment-timezone';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';

const Env = Type.Object({
    'Username': Type.String(),
    'Password': Type.String(),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function posTime(feat: Static<typeof Feature.InputFeature>): Date {
    const time = feat.properties.metadata?.posTime;
    return new Date(typeof time === 'string' || typeof time === 'number' ? time : 0);
}

export default class Task extends ETL {
    static name = 'etl-spider-tracks';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow !== DataFlowType.Incoming) return Type.Object({});
        if (type === SchemaType.Input) return Env;

        return Type.Object({
            ctrId: Type.String(),
            esn: Type.String(),
            fix: Type.String(),
            hdop: Type.Integer(),
            posTime: Type.String({
                format: 'date-time'
            }),
            dataCtrTime: Type.String({
                format: 'date-time'
            }),
            src: Type.String(),
            unitId: Type.String(),
            trackId: Type.String()
        });
    }

    async control(): Promise<void> {
        const env = await this.env(Env);

        const res = await fetch('https://apigw.spidertracks.io/go/aff/feed', {
            method: 'POST',
            headers: {
                Authorization: `Basic ${btoa(env.Username + ':' + env.Password)}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                type: 'dataRequest',
                dataCenter: [{
                    affVer: "json 1.0",
                    name: "AFF",
                    reqTime: new Date().toISOString()
                }],
                msgRequest: [{
                    to: 'spidertracks',
                    from: `CloudTAK-${env.Username}`,
                    msgType: 'dataRequest',
                    dataCtrTime: moment().subtract(1, 'hour').toISOString()
                }]
            })
        })

        if (!res.ok) throw new Error(await res.text());

        const body = await res.typed(Type.Object({
            type: Type.Literal('FeatureCollection'),
            dataInfo: Type.Array(Type.Object({
                  affVer: Type.String(),
                  provider: Type.String(),
                  rptTime: Type.String(),
                  reqTime: Type.String(),
                  sysId: Type.String()
            })),
            features: Type.Array(Type.Object({
                type: Type.Literal('Feature'),
                properties: Type.Unknown(),
                geometry: Type.Object({
                    type: Type.Literal('Point'),
                    coordinates: Type.Array(Type.Number())
                })
            }))
        }))

        const latest: Map<string | number, Static<typeof Feature.InputFeature>> = new Map();
        body.features.forEach((feat) => {
            if (!isObject(feat.properties)) return;
            const props = feat.properties;
            const unitId = props.unitId;
            if (typeof unitId !== 'string' && typeof unitId !== 'number') return;

            const processed: Static<typeof Feature.InputFeature> = {
                id: String(unitId),
                type: 'Feature',
                properties: {
                    course: typeof props.cog === 'number' ? props.cog : undefined,
                    speed: typeof props.speed === 'number' ? props.speed : undefined,
                    metadata: {
                        ctrId: props.ctrId,
                        esn: props.esn,
                        fix: props.fix,
                        hdop: props.hdop,
                        posTime: props.posTime,
                        dataCtrTime: props.dataCtrTime,
                        src: props.src,
                        unitId,
                        trackId: props.trackId
                    }
                },
                geometry: feat.geometry
            }

            const previous = latest.get(unitId);
            if (!previous || posTime(previous) < posTime(processed)) {
                latest.set(unitId, processed);
            }
        });

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: Array.from(latest.values())
        }

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}

